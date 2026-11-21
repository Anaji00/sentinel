import asyncio

from services.agents import comment_letter_job as cj


def test_the_comment_letter_job_does_nothing_when_switched_off(monkeypatch):
    monkeypatch.setenv("COMMENT_LETTER_JOB", "0")
    job = cj.CommentLetterJob.__new__(cj.CommentLetterJob)

    async def boom(*a, **k):
        raise AssertionError("the job must not run a pass while switched off")

    job.run_once = boom
    assert asyncio.run(asyncio.wait_for(job.run(), 5)) is None
