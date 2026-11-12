# Bar-less instruments (investigated 11 Nov 2026)

The reaction study found 12,120 counted instruments from 467,836 episodes. 4,296 of them had no price series: 3,978 not on
Alpaca's active list (counted untradeable) and 318 on it (queued for a daily backfill). Scripts: `scripts/investigate_barless.py`,
`probe_untradeable.py`, `probe_needs_daily.py`, `probe_recoverable.py`.

## What the 3,978 untradeable names are
| Group | Names | Episodes | Note |
|---|---|---|---|
| Plain tickers on no Alpaca list (old, acquired or delisted: AAXN, ABAX, ACAS) | 2,189 | 7,184 | no free source holds their history |
| Delisted, last traded OTC (Alpaca inactive list) | 497 | 2,316 | |
| Delisted, last on an exchange (inactive list) | 209 | 1,032 | Alpaca returns daily history for only 35 |
| Foreign exchange codes and other odd symbols (005930, 600028, 2388, TSX:D) | about 873 | about 2,300 | foreign listings, not tradable here |
| Foreign OTC lines (F or Y ending) | 85 | 225 | |
| Mutual funds (X ending) | 84 | 247 | |
| Symbols that clean to an active ticker ((CALX), [DXPE], NASDAQ:SVC) | 16 | 67 | parser leaves brackets and prefixes |
| Share-class aliases (BRK, BF, PORT, TRTN) | 4 | 42 | Alpaca lists BRK.A and BRK.B, not BRK |

Episodes lost: 13,561 of 467,836 (2.9%) untradeable, 1,357 (0.3%) in the queue. Insider and news families carry them; 8-K and
13F families barely appear.

## The 318 queued names
277 are OTC (mostly unsponsored ADRs such as EADSY, BYDDY, VWAGY). Alpaca's free feed serves daily bars for 101 of the 319
queued: 60 of the 277 OTC names and 41 of the 42 exchange-listed ones. So about 218 will never fill, and the 41 exchange-listed
names fill on the next backfill pass. "Listed by Alpaca" is not "has bars": `priceable_now` treats every listed stock as
priceable.

## Not done
- Exchange is not in the asset meta, so OTC names cannot be told apart in the count. Adding it as a fifth list element would make
  the count honest; nothing reads past index 3.
- Cleaning brackets and exchange prefixes at the source and mapping BRK to BRK.B recovers about 20 names and 109 episodes.
- The earlier "1,937 against 3,146" figures cannot be reproduced: the study population has since grown.
