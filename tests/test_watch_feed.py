"""The surveillance watch feed's classifier: precise, so the feed is read."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import watch_feed as wf  # noqa: E402


def test_real_surveillance_headlines_match() -> None:
    assert wf.topics("AI surveillance startup Flock to cut several hundred jobs amid backlash") == ["plate readers"]
    assert wf.topics("Texas stops issuing permits for Flock cameras along state roads") == ["plate readers"]
    assert wf.topics("Flock's AI tracking sparks privacy fears") == ["plate readers"]
    assert "data brokers" in wf.topics("Data Broker Radaris Loses Domains in Privacy Fight")
    assert "phone tracking" in wf.topics("Police used a cell-site simulator at the protest")
    assert "face recognition" in wf.topics("City council bans facial recognition")
    assert "federal surveillance" in wf.topics("ICE signs new surveillance contract with Palantir")
    assert "police drones" in wf.topics("Town approves drone as first responder program")


def test_ordinary_news_does_not_match() -> None:
    assert wf.topics("Young Indians flock to EU as immigration curbs bite") == []
    assert wf.topics("Japanese delegations flock to China") == []
    assert wf.topics("Japanese Delegations Flock To China") == []                # headline case, no context
    assert wf.topics("Russia launches drones at Kyiv overnight") == []
    assert wf.topics("Ice storm hits Texas, roads closed") == []
    assert wf.topics("Ring of fire eclipse visible tonight") == []
    # Measured false positives from 30 days of collected news.
    assert wf.topics("UBS resets Palantir stock price target for the rest of 2026") == []
    assert wf.topics("U.S. Navy, Boeing Ink First MQ-25A Stingray Production Contract") == []
    assert wf.topics("A Brooklyn bodega cat was stolen; surveillance camera footage shows the thief") == []
    assert wf.topics("New Android malware encrypts files, steals data") == []
    assert "federal surveillance" in wf.topics("ICE expands Palantir contract for immigration tracking")
    assert "phone tracking" in wf.topics("Journalists targeted with Paragon spyware")


def test_every_item_needs_a_topic_civic_sources_read_more() -> None:
    assert wf.include("404media", "Scientists Detect Record-Breaking Radio Signal") == (False, [])
    assert wf.include("eff", "Resisting federal data consolidation", "how location data flows to agencies")[0]
    assert wf.include("cnbc_world", "Luxury developers flocked to Hungary") == (False, [])
    ok, t = wf.include("guardian_world", "Texas stops issuing permits for Flock cameras")
    assert ok and t == ["plate readers"]


def test_general_news_is_judged_on_its_headline() -> None:
    noisy = "A wide-ranging piece that mentions data brokers in passing."
    assert wf.include("reddit", "Looking for feedback on a vessel sanctions report", noisy) == (False, [])
    assert wf.include("epic", "Agency won't say how it got the data", noisy) == (True, ["data brokers"])
