"""Joint Benjamini-Hochberg across every cell of the three free-dataset studies (the registered family). Reads the JSON files each study writes with --out.

    python scripts/study_free_datasets_combine.py reports/study_cftc_cot.json reports/study_treasury_auctions.json reports/study_reg_sho.json
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils.event_study import combine  # noqa: E402

if __name__ == "__main__":
    sets = []
    for p in sys.argv[1:]:
        rows = json.loads(Path(p).read_text(encoding="utf-8"))
        for r in rows:
            r["cell"] = f"{Path(p).stem.replace('study_', '')}:{r['cell']}"
        sets.append(rows)
    for r in combine(sets):
        print(json.dumps(r))
