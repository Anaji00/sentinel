from shared.utils.watch_strip import build_row


def test_a_full_row_carries_each_source_and_a_missing_one_is_null_not_zero():
    quote = {"price": 167.4, "change_pct": 0.75, "session": "after_hours", "delayed": False, "as_of": "2026-11-12T23:35:00+00:00"}
    r = build_row("NVDA", quote, {"date": "2026-11-17", "estimated": False}, {"percentile": 0.973, "as_of": "2026-11-12"}, 10,
                  {"label": "UNEXPLAINED_DISLOCATION", "action": "watch", "alert_at": "t", "reasons": ["x"]})
    assert r["price"] == 167.4 and r["next_earnings"] == {"date": "2026-11-17", "estimated": False}
    assert r["model_percentile"] == 0.973 and r["model_as_of"] == "2026-11-12" and r["held_back_count"] == 10
    # Deliberate update (layer audit 18 Nov, finding 4): the regime now also carries action_supported and action_label for display.
    assert r["regime"] == {"label": "UNEXPLAINED_DISLOCATION", "action": "watch", "alert_at": "t", "action_supported": True, "action_label": "watch"}
    empty = build_row("ZZZZ", None, None, None, None, None)
    assert empty["ticker"] == "ZZZZ" and all(empty[k] is None for k in
           ("price", "change_pct", "next_earnings", "model_percentile", "model_as_of", "held_back_count", "regime"))


def test_a_model_date_is_not_shown_without_a_percentile():
    assert build_row("A", None, None, {"as_of": "2026-11-12"}, 0, None)["model_as_of"] is None
    assert build_row("A", None, None, None, 0, None)["held_back_count"] == 0                # zero held back is a real answer
