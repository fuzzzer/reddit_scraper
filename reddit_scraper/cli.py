# reddit_scraper/cli.py
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from reddit_scraper.logging_setup import setup_logging
from reddit_scraper.services.csv_export import ndjson_to_csv
from reddit_scraper.services.scraper import Scraper
from reddit_scraper.services.txt_export import ndjson_to_txt

# ---------- constants ---------------------------------------------------- #
ROOT = Path(__file__).parent.parent
OUT_BASE = ROOT / "outputs"
OUT_BASE.mkdir(parents=True, exist_ok=True)
(OUT_BASE / "data").mkdir(parents=True, exist_ok=True)
(OUT_BASE / "progress").mkdir(parents=True, exist_ok=True)
# csv & txt dirs are created lazily


# ---------- helpers ------------------------------------------------------ #
def _slug(date: str) -> str:
    return date.replace("-", "_")


def build_paths(sub: str, start: str, end: str) -> dict[str, Path]:
    tag = f"{sub}_{_slug(start)}__{_slug(end)}"
    txt_root = OUT_BASE / "txt"
    return {
        "tag": tag,
        "ndjson":  OUT_BASE / "data"     / f"output_{tag}.ndjson",
        "progress": OUT_BASE / "progress" / f"progress_{tag}.sqlite",
        "csv_sub": OUT_BASE / "csv"      / f"output_{tag}_submissions.csv",
        "csv_com": OUT_BASE / "csv"      / f"output_{tag}_comments.csv",
        "txt_dir": txt_root / f"conversations_{tag}",                 # per-post txt
        "merged":  txt_root / f"all_conversations_{tag}.txt",         # merged result
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="reddit-scraper",
        description="Fetch subreddit submissions (and comments) for a date range.",
    )
    p.add_argument("subreddit")
    p.add_argument("start_date")
    p.add_argument("end_date")
    p.add_argument("--min-score", type=int)
    p.add_argument("--flair")
    p.add_argument("--keywords", help="Comma-separated keywords to search for.")
    p.add_argument("--csv",    action="store_true", help="Also export CSVs")
    p.add_argument("--txt",    action="store_true", help="Export per-post TXT files")
    p.add_argument("--merged", action="store_true",
                   help="With --txt, merge all TXT into one file")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    p.add_argument("--log-file")
    return p.parse_args()


# ---------- main --------------------------------------------------------- #
def main() -> None:
    args = parse_args()
    setup_logging(args.log_level, args.log_file)
    lg = logging.getLogger(__name__)

    flair_list: Optional[List[str]] = (
        [f.strip() for f in args.flair.split(",")] if args.flair else None
    )
    # ADDED: Process keywords argument
    keyword_list: Optional[List[str]] = (
        [k.strip() for k in args.keywords.split(",")] if args.keywords else None
    )

    paths = build_paths(args.subreddit, args.start_date, args.end_date)

    # ... (lazy dir creation is unchanged) ...

    # run scraper ---------------------------------------------------------
    scraper = Scraper(
        subreddit=args.subreddit,
        start_date=args.start_date,
        end_date=args.end_date,
        output=paths["ndjson"],
        min_score=args.min_score,
        flairs=flair_list,
        # ADDED: Pass keywords to the scraper
        keywords=keyword_list,
        progress_db=paths["progress"],
    )

    posts_saved_count = scraper.run()

    if posts_saved_count == 0:
        lg.info("No new posts were saved, skipping export steps.")
        return

    # CSV -----------------------------------------------------------------
    if args.csv:
        lg.info("CSV export …")
        ndjson_to_csv(paths["ndjson"], paths["csv_sub"], paths["csv_com"])
        lg.info("CSV ready in %s", paths["csv_sub"].parent)

    # TXT -----------------------------------------------------------------
    if args.txt or args.merged:
        lg.info("TXT export …")
        ndjson_to_txt(paths["ndjson"], paths["txt_dir"])
        lg.info("TXT files in %s", paths["txt_dir"])

    # merged --------------------------------------------------------------
    if args.merged:
        lg.info("Merging TXT files …")
        merge_script = ROOT / "scripts" / "merge_contents.py"
        subprocess.run(
            [
                sys.executable,
                str(merge_script),
                str(paths["txt_dir"]),
                "--ext", ".txt",
                "--out-dir", str(paths["txt_dir"].parent),  # outputs/txt
            ],
            check=True,
        )
        # Rename the auto-created file to all_conversations_<tag>.txt
        generated = paths["txt_dir"].parent / f"{paths['txt_dir'].name}.txt"
        if generated.exists():
            generated.rename(paths["merged"])
        lg.info("Merged → %s", paths["merged"])


if __name__ == "__main__":
    main()