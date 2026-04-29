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

class PatternLibrary:
    def __init__(self):
        self._db = get_timescale()
    
    def find_similar(
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
            rows = self._db.query("""
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
                  AND c.rule_id = %s
                ORDER BY s.created_at DESC
                LIMIT %s
            """, (rule_id, limit))
            
            remaining_limit = limit - len(rows)
            if remaining_limit > 0:
                # CRITICAL THINKING: Two-Stage Retrieval (Fuzzy Match Second).
                # If we don't have enough exact matches, we backfill the list using "Tag Overlap".
                # Even if the rule_id is different, an event sharing tags like ['strait_of_hormuz', 'tanker'] 
                # might provide the LLM with valuable geopolitical precedent.
                
                extra = self._db.query("""
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
                      -- BEGINNER EXPLANATION: The '&&' operator in PostgreSQL means "Array Overlap".
                      -- It checks if the array of tags in the database shares ANY elements 
                      -- with the array of tags we passed in (%s). It's much faster than looping in Python!
                      AND c.tags && %s::text[]
                      AND c.rule_id != %s
                    ORDER BY s.created_at DESC
                    LIMIT %s
                """, (list(tags), rule_id, int(remaining_limit)))
                rows += extra
            return [self._format_pattern(r) for r in rows]
        
        except Exception as e:
            logger.error(f"Error fetching similar patterns: {e}")
            return []
        
    def record_outcome(
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
            self._db.execute("""
                UPDATE scenarios
                SET status     = %s,
                    updated_at = NOW()
                WHERE scenario_id = %s::uuid
            """, (status, scenario_id))

            if notes:
                # We log the notes for debugging and auditability. 
                # The actual AI reasoning history is appended directly in the scenario_tracker.
                logger.info(f"Recording pattern outcome for scenario {scenario_id}: {status}. Notes: {notes}")
        except Exception as e:
            logger.error(f"Error recording pattern outcome for scenario {scenario_id}: {e}")
    
    def _format_pattern(self, row: Dict) -> Dict:
        return {
            "scenario_id":  str(row.get("scenario_id", "")),
            "headline":     row.get("headline", ""),
            "outcome":      row.get("status", ""),
            "confidence":   row.get("confidence_overall"),
            "rule":         row.get("rule_id", ""),
            "tags":         row.get("correlation_tags", []),
            "description":  row.get("description", ""),
            "date":         row["created_at"].isoformat() if row.get("created_at") else "",
        }