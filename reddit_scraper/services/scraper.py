# reddit_scraper/services/scraper.py
from __future__ import annotations

import logging
import signal
import sys
from pathlib import Path
from typing import List, Optional

from tqdm import tqdm

from reddit_scraper.core.models import Submission, export_ndjson
from reddit_scraper.infra.reddit import RedditClient
from reddit_scraper.services.progress import ProgressTracker


class Scraper:
    """
    Coordinator: Reddit feed → full post → JSON → checkpoint DB
    """

    def __init__(
        self,
        subreddit: str,
        start_date: str,
        end_date: str,
        *,
        output: str | Path = "output.ndjson",
        min_score: Optional[int] = None,
        flairs: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        progress_db: str | Path = "progress.sqlite",
    ) -> None:
        self.subreddit = subreddit
        self.start_date = start_date
        self.end_date = end_date
        self.min_score = min_score
        self.flairs = flairs
        self.keywords = keywords
        self.output = Path(output)

        self.reddit = RedditClient()
        self.progress = ProgressTracker(progress_db)
        self.logger = logging.getLogger(f"{__name__}.{subreddit}")
        self._setup_signals()

    # ----------------------------- main loop --------------------------- #
    def run(self) -> int:
        self.logger.info(
            "Starting scrape of r/%s from %s to %s", self.subreddit, self.start_date, self.end_date
        )

        if self.keywords:
            self.logger.info("Searching with keywords: %s", self.keywords)
            # Reddit search API is not precise with dates, using 'year' is a good default.
            submission_iterator = self.reddit.search_submissions(
                self.subreddit,
                keywords=self.keywords,
                time_filter="year",
                min_score=self.min_score,
                flairs=self.flairs,
            )
        else:
            self.logger.info("Listing recent posts from /new")
            submission_iterator = self.reddit.list_submission_ids(
                self.subreddit,
                self.start_date,
                self.end_date,
                min_score=self.min_score,
                flairs=self.flairs,
            )

        bar = tqdm(unit="posts", desc="Downloaded")
        try:
            for item in submission_iterator:
                sid = item["id"]
                if self.progress.is_done(sid):
                    self.logger.debug("Skip already-scraped id=%s", sid)
                    continue

                raw_tree = self.reddit.fetch_submission_tree(sid)
                submission = Submission.from_pushshift_reddit(raw_tree)

                export_ndjson([submission], self.output, append=True)
                self.progress.mark_done(sid)

                bar.update()
                self.logger.debug("Saved id=%s  (%d comments)", submission.id, len(submission.comments))

            self.logger.info("Scraping finished – %d new posts saved", bar.n)
            return bar.n
        finally:
            bar.close()
            self.progress.close()

    # ------------------------- graceful exit --------------------------- #
    def _setup_signals(self) -> None:
        def _handler(sig_num, _frame):
            sig_name = signal.Signals(sig_num).name
            self.logger.warning("Received %s – shutting down gracefully…", sig_name)
            self.progress.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)