"""
shared/utils/xlsx_lite.py

Rows of the first worksheet of an .xlsx file, standard library only: an xlsx
is a zip of XML parts, and reading values (no formulas, styles or dates as
dates) needs nothing more. Used for government spreadsheets such as ICE's
287(g) participating-agency list.
"""
from __future__ import annotations

import io
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import List, Optional

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_COL = re.compile(r"([A-Z]+)")


def _col_index(ref: str) -> int:
    letters = _COL.match(ref)
    n = 0
    for ch in letters.group(1) if letters else "A":
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def read_rows(data: bytes, sheet: int = 1) -> List[List[Optional[str]]]:
    """Every row of worksheet `sheet` (1-based) as strings; empty cells None."""
    z = zipfile.ZipFile(io.BytesIO(data))
    shared: List[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", _NS):
            shared.append("".join(t.text or "" for t in si.iter(f"{{{_NS['m']}}}t")))
    root = ET.fromstring(z.read(f"xl/worksheets/sheet{sheet}.xml"))
    rows: List[List[Optional[str]]] = []
    for row in root.iter(f"{{{_NS['m']}}}row"):
        cells: List[Optional[str]] = []
        for c in row.findall("m:c", _NS):
            i = _col_index(c.get("r", "A"))
            while len(cells) < i:
                cells.append(None)
            t = c.get("t")
            if t == "inlineStr":
                val: Optional[str] = "".join(x.text or "" for x in c.iter(f"{{{_NS['m']}}}t"))
            else:
                v = c.find("m:v", _NS)
                val = v.text if v is not None else None
                if t == "s" and val is not None:
                    val = shared[int(val)]
            cells.append(val)
        rows.append(cells)
    return rows


__all__ = ["read_rows"]
