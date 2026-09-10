"""/filings/latest must return filings the platform ingested.

It returned four hand-written ones: an NVIDIA multi-gigawatt foundry agreement
with TSMC, a Microsoft EVP departure from Cloud and AI, a Tesla FSD approval
for European markets, an Apple 10-Q -- each with a fabricated sec.gov URL, each
attributed to a real public company, each returned unchanged to every caller.
The collector had 564 real filings in the database at the time.

Inventing corporate disclosures is not a placeholder problem. A reader has no
way to tell these from filings that happened.
"""
import pytest
from fastapi import HTTPException

from services.api_gateway.routes import filings as mod

INVENTED = (
    "multi-gigawatt",
    "Departure of Executive Vice President",
    "Full Self-Driving commercial deployment",
    "nvda-20260814.htm",
)


class _DB:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    async def query(self, sql, *args):
        self.calls.append((sql, args))
        return self.rows


def _row(ticker="MS", form="424B2", url="https://www.sec.gov/Archives/edgar/data/895421/x.htm"):
    return {
        "primary_entity_id": ticker,
        "primary_entity_name": "MORGAN STANLEY",
        "url": url,
        "filing_data": {
            "ticker": ticker,
            "company_name": "MORGAN STANLEY",
            "form_type": form,
            "filing_date": "2026-09-08",
            "is_material_8k": False,
            "items": [],
            "description": f"SEC Filing: MORGAN STANLEY ({ticker}) filed Form {form}",
            "primary_doc_url": url,
        },
    }


@pytest.mark.anyio
async def test_rows_come_from_the_database():
    db = _DB([_row()])
    out = await mod.get_latest_filings(None, None, 25, user={}, db=db)
    assert len(out) == 1
    assert out[0].ticker == "MS"
    assert "events" in db.calls[0][0] and "type = 'filing'" in db.calls[0][0]


@pytest.mark.anyio
async def test_an_empty_database_returns_nothing_not_a_fixture():
    out = await mod.get_latest_filings(None, None, 25, user={}, db=_DB([]))
    assert out == []


@pytest.mark.anyio
async def test_none_of_the_invented_filings_can_still_appear():
    out = await mod.get_latest_filings(None, None, 25, user={}, db=_DB([_row()]))
    blob = " ".join(f.summary + f.primary_doc_url for f in out)
    for phrase in INVENTED:
        assert phrase not in blob


@pytest.mark.anyio
async def test_a_filing_with_no_source_document_is_dropped():
    """A summary the reader cannot check is what the fixtures were."""
    r = _row()
    r["url"] = None
    r["filing_data"]["primary_doc_url"] = None
    assert await mod.get_latest_filings(None, None, 25, user={}, db=_DB([r])) == []


@pytest.mark.anyio
async def test_filters_are_pushed_to_the_query_not_applied_to_a_constant():
    db = _DB([_row()])
    await mod.get_latest_filings("8-K", "NVDA", 5, user={}, db=db)
    sql, args = db.calls[0]
    assert "form_type" in sql and "8-K" in args and "NVDA" in args


@pytest.mark.anyio
async def test_no_database_is_a_503_not_a_fixture():
    with pytest.raises(HTTPException) as e:
        await mod.get_latest_filings(None, None, 25, user={}, db=None)
    assert e.value.status_code == 503


def test_the_filer_summary_carries_the_synthetic_flag():
    """The report has marked seed portfolios since the 13F audit; the list
    view dropped the field and presented them as filed data."""
    assert "is_synthetic" in mod.FilerSummaryItem.model_fields
    assert mod.FilerSummaryItem.model_fields["is_synthetic"].default is False
