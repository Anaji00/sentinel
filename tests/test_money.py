"""Follow the money: the summaries behind the /money page.

Live on 30 October: the three index giants held 21.6% of NVIDIA, and the same
17 institutions held 35.6% of both NVIDIA and AMD. The customer links behind
"who depends on Walmart" come from 10-K text, and the extractor had resolved
"On" to On Holding and "People" to a stock, so links are kept only when the
named customer is the buyer they resolved to.
"""

from shared.utils import money


def test_concentration_counts_only_companies_with_holders():
    pcts = {
        "AAA": {"BLACKROCK": 7.0, "VANGUARD": 9.0, "STATE_STREET": 4.0, "FIDELITY": 3.0},
        "BBB": {"VANGUARD": 5.0},
        "CCC": {},
    }
    c = money.concentration(pcts)
    assert c["companies"] == 2
    assert c["notable"] == 1  # AAA's 20% clears 15%; BBB's 5% does not
    assert c["top"][0] == {"ticker": "AAA", "big_three_pct": 20.0, "tracked_pct": 23.0}
    assert c["median_big_three_pct"] == 12.5


def test_concentration_of_nothing_is_not_zero():
    assert money.concentration({})["median_big_three_pct"] is None


def test_largest_holder_counts():
    pcts = {"A": {"VANGUARD": 9, "FIDELITY": 3}, "B": {"VANGUARD": 8}, "C": {"FIDELITY": 6, "VANGUARD": 2}}
    rows = money.largest_holder_counts(pcts, {"VANGUARD": "Vanguard Group"})
    assert rows[0] == {"filer_id": "VANGUARD", "filer": "Vanguard Group", "companies": 2, "big_three": True}
    assert rows[1]["filer_id"] == "FIDELITY" and rows[1]["companies"] == 1


def test_rival_overlap_skips_pairs_without_data_and_sorts():
    pcts = {"NVDA": {"VANGUARD": 9.4, "BLACKROCK": 8.0}, "AMD": {"VANGUARD": 9.7, "BLACKROCK": 7.0},
            "UPS": {"VANGUARD": 8.0}, "FDX": {"VANGUARD": 1.0}}
    rows = money.rival_overlaps(pcts)
    assert [r["a"] for r in rows] == ["NVDA", "UPS"]
    assert rows[0]["common_pct"] == 16.4 and rows[0]["shared_holders"] == 2


def test_links_keep_only_named_buyers_and_the_latest_filing():
    rows = [
        {"supplier": "CLX", "customer_name": "Walmart Stores, Inc", "customer_ticker": "WMT", "pct": 25, "filed": "2025-08-01"},
        {"supplier": "CLX", "customer_name": "Walmart Inc.", "customer_ticker": "WMT", "pct": 26, "filed": "2026-08-01"},
        {"supplier": "AMGN", "customer_name": "On", "customer_ticker": "ONON", "pct": 77, "filed": "2026-02-13"},
        {"supplier": "EXPD", "customer_name": "People", "customer_ticker": "WMT", "pct": 10, "filed": "2026-02-13"},
        {"supplier": "ELF", "customer_name": "Target, Walmart, Amazon", "customer_ticker": "TGT", "pct": 18, "filed": "2026-05-01"},
        {"supplier": "MAT", "customer_name": "Walmart", "customer_ticker": "WMT", "pct": None, "filed": "2026-02-01"},
    ]
    links = money.trusted_links(rows)
    # "Walmart Stores, Inc" has a comma but the newer "Walmart Inc." wins anyway.
    assert links == [{"supplier": "CLX", "buyer": "WMT", "buyer_name": "Walmart", "pct": 26.0, "filed": "2026-08-01"}]


def test_dependence_groups_need_two_suppliers():
    links = [
        {"supplier": "CLX", "buyer": "WMT", "buyer_name": "Walmart", "pct": 26.0, "filed": "x"},
        {"supplier": "CAG", "buyer": "WMT", "buyer_name": "Walmart", "pct": 29.0, "filed": "x"},
        {"supplier": "AMKR", "buyer": "AAPL", "buyer_name": "Apple", "pct": 29.8, "filed": "x"},
    ]
    groups = money.dependence_by_buyer(links)
    assert [g["buyer"] for g in groups] == ["WMT"]
    assert [s["ticker"] for s in groups[0]["suppliers"]] == ["CAG", "CLX"]


def test_mutual_pairs_once_each():
    rows = [
        {"holder": "A", "held": "B", "pct": 5.0, "reverse_pct": 2.0},
        {"holder": "B", "held": "A", "pct": 2.0, "reverse_pct": 5.0},
        {"holder": "A", "held": "C", "pct": 1.0, "reverse_pct": None},
    ]
    assert [(r["holder"], r["held"]) for r in money.mutual_pairs(rows)] == [("A", "B")]


def test_a_percentage_shared_by_several_buyers_is_a_combined_figure():
    # Roku's 10-K: Walmart and Target both came back at 81% -- the share of
    # its top retailers together, not either one's.
    rows = [
        {"supplier": "ROKU", "customer_name": "Walmart", "customer_ticker": "WMT", "pct": 81, "filed": "2026-02-13"},
        {"supplier": "ROKU", "customer_name": "Target", "customer_ticker": "TGT", "pct": 81, "filed": "2026-02-13"},
        {"supplier": "CLX", "customer_name": "Walmart Inc.", "customer_ticker": "WMT", "pct": 26, "filed": "2026-08-01"},
    ]
    assert [l["supplier"] for l in money.trusted_links(rows)] == ["CLX"]


# ── board interlocks ─────────────────────────────────────────────────────────
#
# Live on 30 October the top "interlocks" were directors of BlackRock's own
# closed-end funds (BTZ, BLW, CII, BDJ) and Saba Capital's 10% stakes in funds:
# true filings, but one fund family's boards, not many companies'.


def test_funds_are_not_operating_companies():
    profiles = [
        {"ticker": "NVDA", "sic": 3674},
        {"ticker": "DSM", "sic": 0},
        {"ticker": "CEF", "sic": 6726},
        {"ticker": "OEF", "sic": 6722},
        {"ticker": "O", "sic": 6798},  # a REIT is an operating company
        {"ticker": "X", "sic": None},
    ]
    assert money.operating_tickers(profiles) == {"NVDA", "O"}


def test_interlocks_keep_only_people_on_two_operating_companies():
    rows = [
        {"person": "HUBBARD ROBERT GLENN", "companies": ["BTZ", "BLW", "CII", "MET"], "roles": ["DIRECTOR"]},
        {"person": "DOE JANE", "companies": ["NVDA", "AMD", "DSM"], "roles": ["DIRECTOR"]},
        {"person": "SABA CAPITAL MANAGEMENT, L.P.", "companies": ["DSM", "LEO"], "roles": ["10% OWNER"]},
    ]
    out = money.operating_interlocks(rows, {"NVDA", "AMD", "MET"})
    assert out == [{"person": "Jane Doe", "companies": ["NVDA", "AMD"], "roles": ["DIRECTOR"]}]


def test_names_read_like_names():
    assert money.display_name("FLORES LORENZO") == "Lorenzo Flores"
    assert money.display_name("SABA CAPITAL MANAGEMENT, L.P.") == "Saba Capital Management, L.P."
    assert money.display_name("BANK OF AMERICA CORP /DE/") == "Bank of America Corp"
    assert money.display_name("Elon Musk") == "Elon Musk"  # already readable: left alone
    assert money.display_name("WARD STEPHEN M JR") == "Stephen M Ward Jr"
    assert money.display_name("WARD JR STEPHEN M") == "Stephen M Ward Jr"  # how EDGAR writes it
    assert money.display_name("SMITH JOHN III") == "John Smith III"


def test_company_label_prefers_the_spelling_a_reader_knows():
    assert money.company_label(["CHURCH DWIGHT", "CHURCH & DWIGHT CO INC /DE/"]) == "Church & Dwight"
    assert money.company_label(["CAMPBELL", "CAMPBELL S", "CAMPBELL'S CO"]) == "Campbell's"
    assert money.company_label(["KRAFT HEINZ CO", "KRAFT HEINZ"]) == "Kraft Heinz"
    assert money.company_label([]) == ""


def test_acronyms_brands_and_missing_titles():
    assert money.company_label(["EOG RESOURCES INC"]) == "EOG Resources"
    assert money.company_label(["DEERE & CO"]) == "Deere"
    assert money.company_label(["KEURIG DR PEPPER INC."]) == "Keurig Dr Pepper"
    assert money.company_name("FDX", ["FEDEX CORP"]) == "FedEx"
    assert money.company_name("F", []) == "Ford"
    assert money.company_name("ZZZZ", []) == "ZZZZ"
