from __future__ import annotations

from pathlib import Path
import re


def test_templates_do_not_directly_strftime_clock_time():
    root = Path(__file__).resolve().parents[1] / "src" / "lamella" / "web" / "templates"
    bad: list[str] = []
    pat = re.compile(r"strftime\([^\n]*%H:[^\n]*\)")
    for p in root.rglob("*.html"):
        text = p.read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), start=1):
            if pat.search(line):
                bad.append(f"{p.relative_to(root)}:{i}: {line.strip()}")
    assert not bad, "Use |local_ts for time-of-day rendering:\n" + "\n".join(bad)
