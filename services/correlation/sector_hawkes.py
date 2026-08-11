"""
services/correlation/sector_hawkes.py

INTRA-TRADFI SECTOR HAWKES CORRELATOR
======================================
A scoped Hawkes process fitter for intra-tradfi sector-level excitation.

Uses 11 GICS sector buckets instead of the 8 global Sentinel domains,
keeping the parameter space manageable (11x11 = 121 α-parameters vs
7,700+ if sectors were folded into the global matrix).

This runs as a completely separate fitter from the global
CrossDomainHawkesCorrelator to avoid parameter blowup and
spectral-radius convergence issues.

Answers questions like: "are semis exciting biotech right now?"
"""

import logging
from typing import Dict, List, Optional, Any
from services.correlation.hawkes_correlator import HawkesMLE

logger = logging.getLogger("correlation.sector_hawkes")

# Standard GICS sectors
GICS_SECTORS = [
    "Technology",
    "Healthcare",
    "Financials",
    "Consumer Cyclical",
    "Communication Services",
    "Industrials",
    "Consumer Defensive",
    "Energy",
    "Utilities",
    "Real Estate",
    "Basic Materials",
]


class IntraTradFiHawkesCorrelator:
    """
    Sector-level Hawkes process for intra-tradfi cross-excitation analysis.
    
    Operates on GICS sector buckets. Completely independent from the global
    8-domain CrossDomainHawkesCorrelator to avoid parameter-count blowup.
    """

    def __init__(
        self,
        sectors: Optional[List[str]] = None,
        learning_rate: float = 1e-3,
        max_iter: int = 200,
        spectral_radius_cap: float = 0.95,
    ):
        self.sectors = sectors or GICS_SECTORS
        self.mle = HawkesMLE(
            domains=self.sectors,
            learning_rate=learning_rate,
            max_iter=max_iter,
            spectral_radius_cap=spectral_radius_cap,
        )
        self._sector_idx = {s: i for i, s in enumerate(self.sectors)}

    def fit(self, sector_event_streams: Dict[str, List[float]], T: Optional[float] = None) -> Dict[str, Any]:
        """
        Fit the sector-level Hawkes process from historical event timestamps.
        
        Args:
            sector_event_streams: Dict mapping sector name -> sorted list of event timestamps
            T: Observation window length in seconds
            
        Returns:
            Fit result dict with branching_matrix, mu, alpha, beta, etc.
        """
        return self.mle.fit(sector_event_streams, T=T)

    def get_branching_ratio(self, source_sector: str, target_sector: str) -> float:
        """Get the excitation ratio from source_sector → target_sector."""
        i = self._sector_idx.get(source_sector)
        j = self._sector_idx.get(target_sector)
        if i is None or j is None:
            return 0.0
        if self.mle.beta < 1e-10:
            return 0.0
        return self.mle.alpha[i][j] / self.mle.beta

    def get_top_excitations(self, target_sector: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Get the top-k source sectors that most excite the target sector.
        
        Returns:
            List of dicts: [{"source": "Technology", "ratio": 0.42}, ...]
        """
        j = self._sector_idx.get(target_sector)
        if j is None:
            return []

        ratios = []
        for sector, i in self._sector_idx.items():
            if sector == target_sector:
                continue
            ratio = self.get_branching_ratio(sector, target_sector)
            if ratio > 0.01:
                ratios.append({"source": sector, "ratio": round(ratio, 4)})

        ratios.sort(key=lambda x: x["ratio"], reverse=True)
        return ratios[:top_k]

    def generate_sector_excitation_summary(self) -> Dict[str, Any]:
        """
        Generate a human-readable summary of sector cross-excitation patterns.
        Useful for macro review prompts and dashboard display.
        """
        summary = {}
        for sector in self.sectors:
            top = self.get_top_excitations(sector, top_k=3)
            if top:
                summary[sector] = {
                    "top_exciting_sectors": top,
                    "baseline_intensity": round(
                        self.mle.mu[self._sector_idx[sector]], 6
                    ),
                }
        return summary
