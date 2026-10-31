"""Hosted quota accounting gives back what a request did not use, and scenario
probabilities are read in the forms a model writes them (30 Oct 2026)."""
import asyncio

import pytest

from shared.utils import hosted_llm as hl
from services.reasoning.scenario_generator import HypothesisOutput, as_percent


def test_unused_tokens():
    assert hl.unused_tokens(2300, None) == 2300                                   # not served: all back
    assert hl.unused_tokens(2300, {"prompt_eval_count": 770, "eval_count": 510}) == 1020
    assert hl.unused_tokens(1000, {"prompt_eval_count": 900, "eval_count": 400}) == 0   # never negative
    assert hl.unused_tokens(1000, {}) == 0                                        # no usage reported: keep it


def test_release_returns_tokens_to_the_minute_and_day_buckets():
    q = hl.Quota(rpm=0, rpd=0, tpm=8000, tpd=0)
    hl._local_counts.clear()
    assert asyncio.run(hl._reserve(None, "groq/x", q, 2300)) is None
    asyncio.run(hl._release(None, "groq/x", q, 1020))
    tokens = {k: v for k, v in hl._local_counts.items() if ":t:" in k}
    requests = {k: v for k, v in hl._local_counts.items() if ":r:" in k}
    assert len(tokens) == 2 and set(tokens.values()) == {1280}                    # minute and day: 2300 - 1020
    assert set(requests.values()) == {1}                                          # the request stays counted
    hl._local_counts.clear()


@pytest.mark.parametrize("raw,want", [(45, 45), (45.0, 45), ("45%", 45), (" 30 % ", 30), (0.45, 45),
                                      (1, 1), (0, 0), (130, 100), (-5, 0)])
def test_as_percent(raw, want):
    assert as_percent(raw) == want


def test_a_non_number_is_still_rejected():
    with pytest.raises(Exception):
        HypothesisOutput(label="x", probability="likely", mechanism="m")
    assert HypothesisOutput(label="x", probability="55%", mechanism="m").probability == 55
