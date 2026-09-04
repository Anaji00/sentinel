"""
services/reasoning/pattern_library.py
 
Historical pattern matching.
 
Every confirmed or denied scenario gets logged here.
When a new correlation fires, we search for similar past patterns
to give Claude historical precedent as context.
 
"This looks like the April 2024 case where vessel dark + USO sweep
preceded a Hormuz incident by 11 days — that scenario was confirmed."
 
This is one of SENTINEL's core long-term moats: the pattern library
grows with every resolved scenario and makes future analysis sharper.
"""

import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional
 
from shared.db import get_timescale
 
logger = logging.getLogger("reasoning.patterns")

# CODING CONVICTION: Magic Numbers.
# We pull this out as a constant so it's easy to tune the strictness of historical matches.
SIMILARITY_TAGS_THRESHOLD = 2   # minimum tag overlap to consider a pattern similar

def _as_iso(value) -> str:
    """A timestamp as ISO text, whatever shape the driver returned it in.

    `created_at.isoformat()` assumed a datetime. When the column arrives as a
    string -- which it does, depending on the cursor -- that raises
    AttributeError, which the formatter's `except (TypeError, ValueError)` does
    not catch. It escaped to the caller's handler, which returns [] for any
    failure, so a type mismatch on one field silently disabled precedent
    retrieval for every scenario the system generated. The log said "Error
    fetching similar patterns" and the pipeline carried on without them.
    """
    if not value:
        return ""
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


class PatternLibrary:
    def __init__(self, db_client):
        self._db = db_client
    
    async def find_similar(
        self, 
        tags: List[str],
        rule_id: str,
        limit: int = 5
    ) -> List[Dict]:
        """
        Find historically similar confirmed/denied scenarios.
        Matches by rule_id first, then by tag overlap.
        Returns list of pattern summaries for inclusion in LLM context.
        """
        if not tags:
            return []
        
        try:
            # CRITICAL THINKING: Two-Stage Retrieval (Exact Match First).
            # To give the LLM the best context, we first look for historical alerts 
            # that triggered on the exact same `rule_id`. This is our highest-confidence 
            # match. If a "Vessel Dark" rule fired today, we want "Vessel Dark" examples from the past.
            
            # First try to find patterns with the same rule_id
            rows = await self._db.query("""
                SELECT
                    s.scenario_id,
                    s.headline,
                    s.status,
                    s.confidence_overall,
                    s.created_at,
                    c.rule_id,
                    c.tags AS correlation_tags,
                    c.description
                FROM scenarios s
                JOIN correlations c ON s.correlation_id = c.correlation_id
                WHERE s.status IN ('confirmed', 'denied')
                  AND c.rule_id = $1
                ORDER BY s.created_at DESC
                LIMIT $2
            """, rule_id, limit)
            
            remaining_limit = limit - len(rows)
            if remaining_limit > 0:
                # CRITICAL THINKING: Two-Stage Retrieval (Fuzzy Match Second).
                # If we don't have enough exact matches, we backfill the list using "Tag Overlap".
                # Even if the rule_id is different, an event sharing tags like ['strait_of_hormuz', 'tanker'] 
                # might provide the LLM with valuable geopolitical precedent.
                
                min_overlap = min(SIMILARITY_TAGS_THRESHOLD, max(1, len(tags)))
                extra = await self._db.query("""
                    SELECT
                        s.scenario_id,
                        s.headline,
                        s.status,
                        s.confidence_overall,
                        s.created_at,
                        c.rule_id,
                        c.tags AS correlation_tags,
                        c.description
                    FROM scenarios s
                    JOIN correlations c ON s.correlation_id = c.correlation_id
                    WHERE s.status IN ('confirmed', 'denied')
                      AND (SELECT count(*) FROM unnest(c.tags) t WHERE t = ANY($1::text[])) >= $4
                      AND c.rule_id != $2
                    ORDER BY s.created_at DESC
                    LIMIT $3
                """, list(tags), rule_id, int(remaining_limit), int(min_overlap))
                rows += extra

            # Both outcomes, when both exist.
            #
            # Precedents are ordered by recency alone, and the corpus is 216
            # confirmed against 0 denied -- so every precedent ever injected
            # into a synthesis prompt was a scenario that had been borne out.
            # A model shown only confirmations is being taught that scenarios
            # of this shape come true.
            #
            # Denial only became reachable when the confidence arithmetic was
            # corrected, so the imbalance will persist for a while yet. This
            # reserves room for the minority outcome rather than waiting for it
            # to win on recency, which on these volumes it never would.
            rows = self._balance_outcomes(rows, limit)
            return [self._format_pattern(r) for r in rows]
        
        except Exception as e:
            logger.error(f"Error fetching similar patterns: {e}")
            return []
        
    async def record_outcome(
        self,
        scenario_id: str,
        status: str,
        notes: str,
    ):
        """
        Called by scenario_tracker when a scenario resolves.
        FIXED: Parameter ordering matches the caller in `scenario_tracker.py` 
        (scenario_id, status, notes).
        """
        try: 
            # This query updates scenarios based on the scenario_id, which is the primary key. 
            # It's a single-row update, so it's efficient and won't cause performance issues.
            await self._db.execute("""
                UPDATE scenarios
                SET status     = $1,
                    updated_at = NOW()
                WHERE scenario_id = $2::uuid
            """, status, scenario_id)

            if notes:
                # We log the notes for debugging and auditability. 
                # The actual AI reasoning history is appended directly in the scenario_tracker.
                logger.info(f"Recording pattern outcome for scenario {scenario_id}: {status}. Notes: {notes}")
        except Exception as e:
            logger.error(f"Error recording pattern outcome for scenario {scenario_id}: {e}")
    
    @staticmethod
    def _balance_outcomes(rows: list, limit: int) -> list:
        """Keeps both outcomes represented, in recency order within each.

        A minority outcome gets up to half the slots; anything it does not use
        goes back to the majority, so a corpus with one outcome is returned
        unchanged rather than truncated.
        """
        confirmed = [r for r in rows if str(r.get("status")) == "confirmed"]
        denied = [r for r in rows if str(r.get("status")) == "denied"]
        if not confirmed or not denied:
            return rows[:limit]

        minority, majority = (denied, confirmed) if len(denied) <= len(confirmed) else (confirmed, denied)
        keep_minority = min(len(minority), max(1, limit // 2))
        selected = minority[:keep_minority] + majority[: limit - keep_minority]
        # Recency order restored across the combined set.
        selected.sort(key=lambda r: r.get("created_at") or 0, reverse=True)
        return selected[:limit]

    def _format_pattern(self, row) -> Dict:
        """Safely formats the row regardless of cursor type."""
        try:
            # Explicitly cast DictRow to standard dict to bypass isinstance failures
            row_dict = dict(row)
            return {
                "scenario_id":  str(row_dict.get("scenario_id", "")),
                "headline":     row_dict.get("headline", ""),
                "outcome":      row_dict.get("status", ""),
                "confidence":   row_dict.get("confidence_overall"),
                "rule":         row_dict.get("rule_id", ""),
                "tags":         row_dict.get("correlation_tags", []),
                "description":  row_dict.get("description", ""),
                "date":         _as_iso(row_dict.get("created_at")),
            }
        except (TypeError, ValueError, AttributeError, KeyError, IndexError):
            # Fallback for strict tuple cursors
            return {
                "scenario_id":  str(row[0]) if row[0] else "",
                "headline":     row[1] or "",
                "outcome":      row[2] or "",
                "confidence":   row[3],
                "date":         _as_iso(row[4]),
                "rule":         row[5] or "",
                "tags":         row[6] or [],
                "description":  row[7] or ""
            }
