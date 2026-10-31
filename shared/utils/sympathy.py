"""
shared/utils/sympathy.py

Measured sympathy peers: for each stock, the stocks whose daily moves net of
SPY have tracked its own over the last WINDOW sessions.

The stock correlation agent asked a model to name sympathy movers, and each
name became a SYMPATHY_MOVER graph edge: 842 on 29 Oct 2026, XOM's including
BKLN, MAR, ABNB and AZO. Net of the market, what is left in two stocks'
co-movement is mostly their shared industry and inputs, which is what a
sympathy move travels through, so it is measured here and the model only
chooses among measured peers.

Pure: closes in, peer rows out.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd

WINDOW = 120            # sessions of returns
MIN_OVERLAP = 80        # sessions both stocks traded, for a correlation to count
MIN_CORR = 0.35         # below this a peer is not a sympathy mover
TOP_K = 8               # peers kept per stock

PeerRow = Tuple[str, str, float, int]      # (ticker, peer, correlation, sessions)


def excess_returns(closes: Mapping[str, Sequence[Tuple[date, float]]], spy: Sequence[Tuple[date, float]],
                   window: int = WINDOW) -> pd.DataFrame:
    """Daily log returns net of SPY's, one column per stock, the last `window`
    sessions of SPY's calendar; a session a stock did not trade is NaN."""
    s = pd.Series({d: c for d, c in spy if c and c > 0}).sort_index()
    spy_r = np.log(s).diff().iloc[1:].tail(window)
    cols: Dict[str, pd.Series] = {}
    for t, rows in closes.items():
        x = pd.Series({d: c for d, c in rows if c and c > 0}).sort_index()
        if len(x) < 2:
            continue
        r = np.log(x).diff().iloc[1:]
        cols[t] = r.reindex(spy_r.index) - spy_r
    return pd.DataFrame(cols, index=spy_r.index)


def top_peers(excess: pd.DataFrame, min_overlap: int = MIN_OVERLAP, min_corr: float = MIN_CORR,
              k: int = TOP_K) -> List[PeerRow]:
    """For each stock, up to k peers with correlation >= min_corr over at least
    min_overlap shared sessions, strongest first."""
    if excess.shape[1] < 2:
        return []
    corr = excess.corr(min_periods=min_overlap)
    present = excess.notna().astype(float)
    overlap = present.T @ present
    out: List[PeerRow] = []
    for t in corr.columns:
        c = corr[t].drop(labels=[t]).dropna()
        c = c[c >= min_corr].sort_values(ascending=False).head(k)
        for peer, v in c.items():
            if math.isfinite(float(v)):
                out.append((str(t), str(peer), round(float(v), 4), int(overlap.at[t, peer])))
    return out


def peer_lines(rows: Sequence[Mapping[str, object]]) -> List[str]:
    """Prompt lines for a stock's stored peers."""
    return [f"{r['peer']}: daily moves net of SPY correlated {float(r['corr']):+.2f} over {int(r['n'])} sessions"
            for r in rows]


__all__ = ["MIN_CORR", "MIN_OVERLAP", "TOP_K", "WINDOW", "PeerRow", "excess_returns", "peer_lines", "top_peers"]
