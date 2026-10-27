#!/usr/bin/env python3
"""Turn build/test failure details into GitHub Actions error annotations.

Annotations are visible on the run summary (and via the API), which makes failures
readable without downloading the full job log.
Usage: annotate_failures.py <build-log> [<report-glob> ...]
"""
import glob
import re
import sys

PATTERNS = re.compile(r"(ERROR:|\*\*\* FAILED|\*\*\* ABORTED|\[ERROR\]|Exception|error:|Caused by)")


def emit(title: str, lines: list[str]) -> None:
    body = "%0A".join(l.rstrip().replace("%", "%25").replace("\r", "")[:300] for l in lines[:40])
    print(f"::error title={title}::{body}")


def main() -> None:
    log, *globs = sys.argv[1:] or ["build.log"]
    try:
        with open(log, encoding="utf-8", errors="replace") as f:
            hits = [l for l in f if PATTERNS.search(l) and "WARN" not in l]
        if hits:
            emit("Build errors", hits)
    except FileNotFoundError:
        pass
    for pattern in globs or ["**/target/surefire-reports/TestSuite.txt"]:
        for report in glob.glob(pattern, recursive=True):
            with open(report, encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            bad = [i for i, l in enumerate(lines) if "*** FAILED" in l or "*** ABORTED" in l]
            context = []
            for i in bad[:8]:
                context += lines[i:i + 5]
            if context:
                emit(f"Test failures in {report.split('/target/')[0]}", context)


if __name__ == "__main__":
    main()
