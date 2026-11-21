"""Review F8 (18 Nov): a prediction is persisted once per agent, however often it is resolved."""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_the_durable_write_is_idempotent_and_the_index_backs_it() -> None:
    src = (ROOT / "services/agents/prediction_scoring.py").read_text(encoding="utf-8")
    i = src.index("INSERT INTO agent_predictions (")
    assert "ON CONFLICT DO NOTHING" in src[i:i + 900]
    mig = (ROOT / "shared/db/migrate.py").read_text(encoding="utf-8")
    j = mig.index('"0100_agent_prediction_unique"')
    block = mig[j:j + 2600]
    assert "CREATE UNIQUE INDEX IF NOT EXISTS agent_pred_unique_prediction" in block
    assert block.index("INSERT INTO agent_predictions_dupes_19nov") < block.index("DELETE FROM agent_predictions")   # copied before anything is removed
    assert "wargame_sim" in block                                                                                    # the telemetry worker's literal id is outside the index
