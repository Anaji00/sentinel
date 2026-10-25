"""
shared/utils/bar_feed.py

Which exchange feed the daily bars come from, and what their volume is a share of.

Daily history is consolidated (SIP) from 8 October 2026: Alpaca's free plan
serves every exchange's bars once they are fifteen minutes old. Before that it
was IEX, whose daily close is IEX's last trade (25-40 bps from the official
close on low-priced names) and whose volume is 2-7% of the tape by stock. Every
dollar-volume floor and cost estimate read from daily bars scales by
DAILY_BAR_VOLUME_SHARE, so one setting moves them all together with the data.
Minute bars backfill from SIP too (15 Oct 2026); only the newest half hour is
the live stream's provisional bars, settled to SIP as they age.
"""
import os

IEX_VOLUME_SHARE = 0.025          # IEX's typical share of the consolidated tape
DAILY_FEED = os.getenv("ALPACA_DAILY_FEED", "sip")
DAILY_BAR_VOLUME_SHARE = IEX_VOLUME_SHARE if DAILY_FEED == "iex" else 1.0

__all__ = ["DAILY_BAR_VOLUME_SHARE", "DAILY_FEED", "IEX_VOLUME_SHARE"]
