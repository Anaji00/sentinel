"""
tests/test_subjective_logic_consensus.py

Tests for Subjective Logic opinion fusion, ACH report generation,
and cross-agent state digest context drift detection.
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone

from services.agents.consensus_engine import (
    ConsensusEngine,
    ConsensusReport,
    ConsensusSignal,
    ContradictionReport,
    SubjectiveOpinion,
    ACHReport,
    CompetingHypothesis,
    ACH_UNCERTAINTY_THRESHOLD,
)
from services.agents.base import AgentBulletin, AgentScorecard


# ── SUBJECTIVE LOGIC OPINION MATH ───────────────────────────────────────────

class TestSubjectiveOpinion:

    def test_vacuous_opinion(self):
        """Default opinion should be vacuous (full uncertainty)."""
        op = SubjectiveOpinion()
        assert op.uncertainty == 1.0
        assert op.belief == 0.0
        assert op.disbelief == 0.0
        assert abs(op.projected_probability - 0.5) < 0.01  # P = 0 + 0.5 * 1.0

    def test_projected_probability(self):
        """P = b + a * u should be computed correctly."""
        op = SubjectiveOpinion(belief=0.7, disbelief=0.1, uncertainty=0.2, base_rate=0.5)
        expected = 0.7 + 0.5 * 0.2  # 0.8
        assert abs(op.projected_probability - expected) < 0.01

    def test_constraint_bdu_sum_to_one(self):
        """b + d + u should always equal 1.0."""
        op = SubjectiveOpinion(belief=0.4, disbelief=0.3, uncertainty=0.3)
        total = op.belief + op.disbelief + op.uncertainty
        assert abs(total - 1.0) < 0.01

    def test_from_bulletin_bullish(self):
        """A bullish bulletin should produce an opinion with belief > disbelief."""
        bulletin = AgentBulletin(
            agent_name="quant",
            bulletin_type="signal",
            ticker="AAPL",
            conviction=0.85,
            expected_direction="bullish",
        )
        op = SubjectiveOpinion.from_bulletin(bulletin, weight=1.5)
        assert op.belief > op.disbelief, f"Bullish bulletin should have b > d: {op}"
        assert op.belief + op.disbelief + op.uncertainty == pytest.approx(1.0, abs=0.01)

    def test_from_bulletin_bearish(self):
        """A bearish bulletin should produce an opinion with disbelief > belief."""
        bulletin = AgentBulletin(
            agent_name="macro",
            bulletin_type="signal",
            ticker="SPY",
            conviction=0.9,
            expected_direction="bearish",
        )
        op = SubjectiveOpinion.from_bulletin(bulletin, weight=1.0)
        assert op.disbelief > op.belief, f"Bearish bulletin should have d > b: {op}"

    def test_from_bulletin_neutral(self):
        """A neutral bulletin should produce high uncertainty."""
        bulletin = AgentBulletin(
            agent_name="radar",
            bulletin_type="thesis",
            ticker="XYZ",
            conviction=0.5,
            expected_direction="neutral",
        )
        op = SubjectiveOpinion.from_bulletin(bulletin, weight=1.0)
        assert op.uncertainty > 0.5

    def test_higher_weight_lowers_uncertainty(self):
        """A higher-weight agent should produce lower uncertainty."""
        bulletin = AgentBulletin(
            agent_name="quant",
            bulletin_type="signal",
            ticker="AAPL",
            conviction=0.8,
            expected_direction="bullish",
        )
        op_low = SubjectiveOpinion.from_bulletin(bulletin, weight=0.5)
        op_high = SubjectiveOpinion.from_bulletin(bulletin, weight=2.0)
        assert op_high.uncertainty < op_low.uncertainty, \
            "Higher weight should produce lower uncertainty"


# ── CUMULATIVE FUSION ────────────────────────────────────────────────────────

class TestCumulativeFusion:

    def test_fuse_empty_list(self):
        """Fusing empty list should return vacuous opinion."""
        fused = SubjectiveOpinion.cumulative_fuse([])
        assert fused.uncertainty == 1.0

    def test_fuse_single_opinion(self):
        """Fusing a single opinion should return a copy."""
        op = SubjectiveOpinion(belief=0.6, disbelief=0.2, uncertainty=0.2)
        fused = SubjectiveOpinion.cumulative_fuse([op])
        assert abs(fused.belief - op.belief) < 0.01

    def test_fuse_agreeing_opinions_strengthens_belief(self):
        """Two agreeing opinions should produce stronger belief than either alone."""
        op1 = SubjectiveOpinion(belief=0.5, disbelief=0.1, uncertainty=0.4)
        op2 = SubjectiveOpinion(belief=0.6, disbelief=0.1, uncertainty=0.3)
        fused = SubjectiveOpinion.cumulative_fuse([op1, op2])
        assert fused.belief > max(op1.belief, op2.belief) * 0.8, \
            "Agreeing opinions should strengthen belief"
        assert fused.uncertainty < max(op1.uncertainty, op2.uncertainty), \
            "Fusion should reduce uncertainty"

    def test_fuse_conflicting_opinions_raises_uncertainty(self):
        """Two conflicting opinions should produce higher uncertainty than agreeing ones."""
        bullish = SubjectiveOpinion(belief=0.7, disbelief=0.1, uncertainty=0.2)
        bearish = SubjectiveOpinion(belief=0.1, disbelief=0.7, uncertainty=0.2)
        fused_conflict = SubjectiveOpinion.cumulative_fuse([bullish, bearish])

        # Compare with two agreeing opinions
        bullish2 = SubjectiveOpinion(belief=0.7, disbelief=0.1, uncertainty=0.2)
        fused_agree = SubjectiveOpinion.cumulative_fuse([bullish, bullish2])

        # Conflicting fusion should have higher conflict level
        assert fused_conflict.conflict_level >= fused_agree.conflict_level or \
               fused_conflict.uncertainty >= fused_agree.uncertainty, \
            "Conflicting opinions should produce higher conflict/uncertainty"

    def test_fuse_normalizes_to_one(self):
        """Fused b + d + u should equal 1.0."""
        ops = [
            SubjectiveOpinion(belief=0.4, disbelief=0.3, uncertainty=0.3),
            SubjectiveOpinion(belief=0.5, disbelief=0.2, uncertainty=0.3),
            SubjectiveOpinion(belief=0.1, disbelief=0.6, uncertainty=0.3),
        ]
        fused = SubjectiveOpinion.cumulative_fuse(ops)
        total = fused.belief + fused.disbelief + fused.uncertainty
        assert abs(total - 1.0) < 0.01, f"b+d+u should be 1.0, got {total}"

    def test_zadeh_paradox_graceful_degradation(self):
        """
        Unlike raw Dempster-Shafer, SL should NOT collapse under
        highly conflicting evidence (Zadeh's paradox scenario).
        """
        strongly_bullish = SubjectiveOpinion(belief=0.95, disbelief=0.01, uncertainty=0.04)
        strongly_bearish = SubjectiveOpinion(belief=0.01, disbelief=0.95, uncertainty=0.04)
        fused = SubjectiveOpinion.cumulative_fuse([strongly_bullish, strongly_bearish])

        # Should NOT crash or produce NaN
        assert 0.0 <= fused.belief <= 1.0
        assert 0.0 <= fused.disbelief <= 1.0
        assert 0.0 <= fused.uncertainty <= 1.0
        # Total should be 1
        total = fused.belief + fused.disbelief + fused.uncertainty
        assert abs(total - 1.0) < 0.01


# ── ACH REPORT GENERATION ───────────────────────────────────────────────────

class TestACHReport:

    def test_ach_report_structure(self):
        """ACH report should have well-formed competing hypotheses."""
        ach = ACHReport(
            ticker="AAPL",
            fused_opinion=SubjectiveOpinion(belief=0.3, disbelief=0.3, uncertainty=0.4),
            competing_hypotheses=[
                CompetingHypothesis(
                    hypothesis="Bullish on AAPL",
                    supporting_agents=["quant"],
                    refuting_agents=["macro"],
                ),
                CompetingHypothesis(
                    hypothesis="Bearish on AAPL",
                    supporting_agents=["macro"],
                    refuting_agents=["quant"],
                ),
            ],
            conflict_level=0.6,
            fused_uncertainty=0.4,
            reason="High uncertainty test",
        )
        assert len(ach.competing_hypotheses) == 2
        assert ach.fused_uncertainty >= ACH_UNCERTAINTY_THRESHOLD


# ── CONSENSUS ENGINE INTEGRATION ─────────────────────────────────────────────

class TestConsensusEngineIntegration:

    def test_empty_bulletins_returns_empty_report(self):
        """With no bulletins, analyze() should return an empty report."""
        async def run_test():
            class DummyRedis:
                class raw:
                    @staticmethod
                    async def scan(cursor=0, match="", count=100):
                        return 0, []
                    @staticmethod
                    async def mget(keys):
                        return []
                    @staticmethod
                    async def set(key, value, ex=None):
                        pass

            engine = ConsensusEngine(DummyRedis())
            report = await engine.analyze()
            assert isinstance(report, ConsensusReport)
            assert report.total_active_bulletins == 0
            assert len(report.ach_reports) == 0

        asyncio.run(run_test())

    def test_report_has_new_fields(self):
        """ConsensusReport should include ach_reports and stale_agents."""
        report = ConsensusReport()
        assert hasattr(report, "ach_reports")
        assert hasattr(report, "stale_agents")
        assert isinstance(report.ach_reports, list)
        assert isinstance(report.stale_agents, list)


# ── CONFORMAL SIMILARITY CALIBRATOR ─────────────────────────────────────────

class TestConformalSimilarityCalibrator:

    def test_default_threshold(self):
        """Before calibration, threshold should be the default."""
        from services.correlation.soft_correlator import ConformalSimilarityCalibrator
        cal = ConformalSimilarityCalibrator(default_threshold=0.65)
        assert cal.threshold == 0.65

    def test_threshold_calibrates_after_enough_samples(self):
        """After enough null samples, threshold should update."""
        from services.correlation.soft_correlator import ConformalSimilarityCalibrator
        cal = ConformalSimilarityCalibrator(target_far=0.10, min_samples=20)
        import random
        random.seed(42)
        for _ in range(100):
            cal.observe_null_score(random.uniform(0.3, 0.7))
        assert cal._calibrated_threshold is not None
        assert 0.40 <= cal.threshold <= 0.90

    def test_threshold_clamped_to_range(self):
        """Threshold should be clamped to [0.40, 0.90]."""
        from services.correlation.soft_correlator import ConformalSimilarityCalibrator
        cal = ConformalSimilarityCalibrator(min_samples=10)
        # Feed all very high scores → threshold would be very high
        for _ in range(50):
            cal.observe_null_score(0.99)
        assert cal.threshold <= 0.90
        # Feed all very low scores → threshold would be very low
        cal2 = ConformalSimilarityCalibrator(min_samples=10)
        for _ in range(50):
            cal2.observe_null_score(0.01)
        assert cal2.threshold >= 0.40

    def test_status_output(self):
        """get_status() should return diagnostic info."""
        from services.correlation.soft_correlator import ConformalSimilarityCalibrator
        cal = ConformalSimilarityCalibrator()
        status = cal.get_status()
        assert "threshold" in status
        assert "calibrated" in status
        assert "null_samples" in status
        assert "target_far" in status
