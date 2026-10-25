"""The personal privacy plan: answers in, ranked free steps out, nothing stored."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import privacy_plan as pp  # noqa: E402

BROKERS = [{"name": "Fog Data science LLC", "privacy_url": "https://fog.example/privacy"},
           {"name": "No Link Inc", "privacy_url": None}]


def ids(plan):
    return [s["id"] for s in plan]


def test_californians_start_with_drop() -> None:
    plan = pp.build_plan(pp.Answers(state="CA"), BROKERS, True)
    assert ids(plan)[0] == "ca_drop" and "local_records" in ids(plan)
    assert "ca_drop" not in ids(pp.build_plan(pp.Answers(state="TX"), BROKERS, False))


def test_police_brokers_come_with_their_opt_out_links() -> None:
    step = next(s for s in pp.build_plan(pp.Answers(), BROKERS) if s["id"] == "police_brokers")
    assert step["links"] == [{"label": "Fog Data science LLC", "url": "https://fog.example/privacy"}]
    assert "2 brokers" in step["why"]
    assert "police_brokers" not in ids(pp.build_plan(pp.Answers(), []))


def test_concerns_raise_the_steps_that_matter() -> None:
    plan = pp.build_plan(pp.Answers(concerns=["domestic_violence"]), BROKERS)
    assert ids(plan)[:2] == ["dv", "people_search"]
    plan = pp.build_plan(pp.Answers(concerns=["immigration"], phone="android"), BROKERS)
    assert ids(plan)[0] in ("immigration", "police_brokers") and "phone_location" in ids(plan)
    android = next(s for s in plan if s["id"] == "phone_location")["how"]
    assert "Delete advertising ID" in android


def test_device_steps_only_when_relevant() -> None:
    assert "car" not in ids(pp.build_plan(pp.Answers()))
    assert "car" in ids(pp.build_plan(pp.Answers(has_car=True)))
    door = next(s for s in pp.build_plan(pp.Answers(has_doorbell_camera=True)) if s["id"] == "doorbell")
    assert "home address" in door["why"]
    iphone = next(s for s in pp.build_plan(pp.Answers(phone="iphone")) if s["id"] == "phone_location")["how"]
    assert "Allow Apps to Request to Track" in iphone


def test_answers_are_validated() -> None:
    a = pp.parse_answers({"state": "ca", "phone": "nokia", "concerns": ["immigration", "bogus"], "has_car": 1})
    assert a.state == "CA" and a.phone is None and list(a.concerns) == ["immigration"] and a.has_car is True
    assert pp.parse_answers({"state": "1x"}).state is None and pp.parse_answers({}).concerns == []
