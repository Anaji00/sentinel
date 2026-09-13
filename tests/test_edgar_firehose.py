"""The SEC feed sees every filer, not twenty names in a dict.

`BASE_CIK_MAP` holds twenty US mega-caps and the per-CIK poller could never look
outside it, so seven distinct companies had ever produced a filing event on the
running deployment -- while the platform holds 13F positions in hundreds of
issuers and graph nodes for many more. EDGAR publishes the whole accepted-filings
stream; this reads it.

These tests are about the parser, because that is where a firehose breaks
silently: an entry it cannot read is an entry nobody notices missing.
"""
import importlib.util
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

ROOT = Path(__file__).resolve().parents[1]
ATOM = "{http://www.w3.org/2005/Atom}"


def _load():
    spec = importlib.util.spec_from_file_location(
        "edgar_firehose_under_test",
        ROOT / "services" / "collector-filings" / "edgar_firehose.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def fh():
    return _load()


def _entry(xml: str) -> ET.Element:
    feed = f'<feed xmlns="http://www.w3.org/2005/Atom">{xml}</feed>'
    return next(iter(ET.fromstring(feed).iter(f"{ATOM}entry")))


APPLE_8K = """
<entry>
 <title>8-K - APPLE INC (0000320193) (Filer)</title>
 <link rel="alternate" type="text/html"
   href="https://www.sec.gov/Archives/edgar/data/320193/000032019325000073/0000320193-25-000073-index.htm"/>
 <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2025-08-01 &lt;b&gt;AccNo:&lt;/b&gt;
   0000320193-25-000073 &lt;b&gt;Size:&lt;/b&gt; 1 MB Items 2.02,9.01</summary>
 <updated>2025-08-01T16:31:00-04:00</updated>
</entry>
"""


class TestEntryParsing:
    def test_a_hyphenated_form_is_read(self, fh):
        # The form is "8-K". A separator rule that split on the hyphen character
        # rejected every 8-K in the feed -- which is most of it.
        record = fh._parse_entry(_entry(APPLE_8K))
        assert record is not None
        assert record["form"] == "8-K"
        assert record["company_name"] == "APPLE INC"
        assert record["cik"] == "320193"
        assert record["accession_number"] == "0000320193-25-000073"

    def test_amendments_are_read_as_their_own_form(self, fh):
        record = fh._parse_entry(_entry("""
        <entry><title>8-K/A - BETA CORP (0000999888) (Filer)</title>
        <link href="https://www.sec.gov/Archives/edgar/data/999888/x/0000999888-25-000002-index.htm"/>
        <summary>Item 5.02</summary><updated>2025-08-01T11:00:00-04:00</updated></entry>"""))
        assert record["form"] == "8-K/A"
        assert record["items"] == ["5.02"]

    def test_item_codes_come_from_the_items_label(self, fh):
        record = fh._parse_entry(_entry(APPLE_8K))
        assert record["items"] == ["2.02", "9.01"]

    def test_a_file_size_is_not_an_item_code(self, fh):
        # "10.25 MB" matches the shape of an item number. Scanning the whole
        # summary for one would have attached item 0.25 to the filing.
        record = fh._parse_entry(_entry("""
        <entry><title>424B2 - ACME FUNDING TRUST - SERIES A (0001234567) (Filer)</title>
        <link href="https://www.sec.gov/Archives/edgar/data/1234567/x/0001234567-25-000001-index.htm"/>
        <summary>&lt;b&gt;Size:&lt;/b&gt; 10.25 MB</summary>
        <updated>2025-08-01T09:00:00-04:00</updated></entry>"""))
        assert record["items"] == []

    def test_a_company_name_may_contain_the_separator(self, fh):
        record = fh._parse_entry(_entry("""
        <entry><title>424B2 - ACME FUNDING TRUST - SERIES A (0001234567) (Filer)</title>
        <link href="https://www.sec.gov/Archives/edgar/data/1234567/x/0001234567-25-000001-index.htm"/>
        <summary>x</summary><updated>2025-08-01T09:00:00-04:00</updated></entry>"""))
        assert record["company_name"] == "ACME FUNDING TRUST - SERIES A"
        assert record["form"] == "424B2"

    def test_an_unreadable_entry_is_dropped_not_half_published(self, fh):
        assert fh._parse_entry(_entry(
            "<entry><title>no cik here</title><link href='https://x/y'/></entry>"
        )) is None

    def test_an_entry_without_an_accession_is_dropped(self, fh):
        assert fh._parse_entry(_entry(
            "<entry><title>8-K - ACME CORP (0000012345) (Filer)</title>"
            "<link href='https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany'/></entry>"
        )) is None


class TestFeedScope:
    def test_only_forms_the_enricher_handles_are_requested(self, fh):
        # The full stream is dominated by Forms 3/4/5 ownership reports. Asking
        # for everything would bury the material filings in paperwork.
        assert set(fh.FIREHOSE_FORMS) == {"8-K", "S-1", "424B"}

    def test_the_feed_url_carries_the_form_and_count(self, fh):
        url = fh.EDGAR_CURRENT_URL.format(form="8-K", count=fh.FIREHOSE_COUNT)
        assert "action=getcurrent" in url
        assert "type=8-K" in url
        assert "output=atom" in url
