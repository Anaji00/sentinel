"""Prompt engineering across the model calls (30 Oct 2026): the system prompt
reaches hosted models in its own role, answers write their evidence before
their verdict, and an extraction's quote must be in the text it read."""
import pathlib

from shared.utils import activist_13d as a13
from shared.utils import analyst as an
from shared.utils import filing_extract as fx
from shared.utils import hosted_llm as hl
from shared.utils import researcher as rs

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_the_system_prompt_goes_in_the_system_role():
    body = hl.request_body("openai/gpt-oss-20b", "the facts", 0.0, "json", 100, system="You are an analyst.")
    assert body["messages"] == [{"role": "system", "content": "You are an analyst."},
                                {"role": "user", "content": "the facts"}]
    assert hl.request_body("m", "only user", 0.0, None, 10)["messages"] == [{"role": "user", "content": "only user"}]


def test_evidence_is_generated_before_the_verdict():
    def order(model, first, then):
        f = list(model.model_fields)
        return f.index(first) < f.index(then)
    assert order(an.AnalystView, "rationale", "p_beat")
    assert order(fx.Guidance, "evidence", "direction")
    assert order(a13.Intent, "quote", "activist")
    assert order(rs.Idea, "rationale", "op")


def test_a_guidance_change_must_quote_the_release():
    passage = "The company today raised its full-year revenue outlook to $4.1 billion from $3.9 billion."
    ok = fx.Guidance(evidence="raised its full-year revenue outlook to $4.1 billion", direction="raised")
    made_up = fx.Guidance(evidence="we now expect record margins across every segment", direction="raised")
    assert fx.grounded(ok, passage) and not fx.grounded(made_up, passage)
    assert fx.grounded(fx.Guidance(direction="none"), passage)


def test_the_stock_correlation_prompt_asks_for_the_direction_it_is_graded_on():
    src = (ROOT / "services" / "agents" / "stock_correlation_agent.py").read_text(encoding="utf-8")
    assert "3. expected_direction: up, down or neutral" in src
    assert "Active Market Universe Tickers" not in src and "Sympathy Engine" not in src
    assert "self._macro_move(target_macro)" in src


def test_the_analyst_is_not_shown_the_rank_it_is_combined_with():
    src = (ROOT / "services" / "agents" / "analyst_agent.py").read_text(encoding="utf-8")
    assert 'an.facts(ticker, None, "monthly"' in src                 # the rank is the prior, not a fact
    assert "an.prior_from_percentile(rank)" in src                    # and still applied as one
    system, _ = an.prompt("XOM", ["F1. The alert: x"])
    assert "do not count it again" not in system


def test_a_raise_or_cut_must_be_in_the_quote():
    # The audit's misses (30 Oct 2026): a stated outlook and a past result read as "raised".
    assert not fx.direction_supported("raised", "We expect fourth quarter earnings of $2.81 per share.")
    assert not fx.direction_supported("raised", "We exceeded the high end of our third-quarter profit outlook.")
    assert fx.direction_supported("raised", "The company raised its full-year revenue outlook to $4.1 billion.")
    assert fx.direction_supported("lowered", "We are lowering our full-year EPS guidance to $3.10.")
    assert not fx.direction_supported("lowered", "We continue to expect revenue of $2 billion.")
    assert fx.direction_supported("maintained", "We reaffirm our outlook.")


def test_tables_keep_their_rows_and_percentages_are_checked():
    from shared.utils import customer_links as cl
    html = ("<p>Major customers.</p><table><tr><th>Customer</th><th>Share</th></tr>"
            "<tr><td>Walmart Inc.</td><td>21%</td></tr><tr><td>Target Corp</td><td>12%</td></tr></table>")
    text = fx.text_of(html)
    assert "Walmart Inc. | 21%." in text and "Target Corp | 12%." in text
    assert cl.pct_in(21.0, "Walmart accounted for 21.0 % of net sales") and not cl.pct_in(25, "21% of sales")
    got = cl.grounded([{"name": "Walmart Inc.", "pct": 21}, {"name": "Target Corp", "pct": 30}], text)
    assert got == [("Walmart Inc", 21.0), ("Target Corp", None)]        # 30% is not in the text


def test_guidance_said_outright_is_read_by_pattern_and_sized():
    assert fx.clear_direction("Sales grew. The Company raises full-year guidance on demand.")[0] == "raised"
    assert fx.clear_direction("We reaffirm our 2026 outlook.")[0] == "maintained"
    assert fx.clear_direction("We raised our outlook. We lowered our capex forecast.") is None   # mixed: to the model
    assert fx.clear_direction("We expect revenue of $2 billion.") is None
    assert fx.change_pct("raised its revenue outlook to $4.1 billion from $3.9 billion") == 0.0513
    assert fx.change_pct("EPS to a range of $3.10 to $3.20 from $3.00 to $3.10") == 0.0328
    assert fx.change_pct("We expect fourth quarter earnings of $2.81 per share.") is None
