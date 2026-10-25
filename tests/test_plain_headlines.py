"""Feed headlines read as sentences, not as detector output."""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.candles import above_normal_words, window_words  # noqa: E402


def test_a_candle_window_reads_in_hours_and_days() -> None:
    assert window_words(1) == "1 minute"
    assert window_words(30) == "30 minutes"
    assert window_words(60) == "1 hour"
    assert window_words("240") == "4 hours"
    assert window_words(1440) == "1 day"
    assert window_words(90) == "90 minutes"
    assert window_words("x") == "x minutes"


def test_how_unusual_a_reading_is_reads_in_words() -> None:
    assert above_normal_words(2.6) == "above normal"
    assert above_normal_words(3.5) == "well above normal"
    assert above_normal_words(12.8) == "far above normal"


def test_no_feed_headline_template_says_structural_anomaly_or_z_score() -> None:
    sources = [
        ROOT / "services/enrichment/enrichers/crypto.py",
        ROOT / "services/enrichment/enrichers/tradfi.py",
        ROOT / "services/agents/signal_engine.py",
    ]
    for path in sources:
        for line in path.read_text(encoding="utf-8").splitlines():
            if "headline" in line and ("f\"" in line or "f'" in line):
                assert "Structural Anomaly" not in line, f"{path.name}: {line.strip()}"
                assert "Z-Score" not in line, f"{path.name}: {line.strip()}"
                assert "p={" not in line, f"{path.name}: {line.strip()}"
