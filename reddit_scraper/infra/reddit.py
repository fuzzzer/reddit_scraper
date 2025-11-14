# reddit_scraper/infra/reddit.py
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional

import praw
from dotenv import load_dotenv
from praw.exceptions import APIException, RedditAPIException
from prawcore.exceptions import RequestException, ResponseException, ServerError

load_dotenv()  # read .env

class RedditClient:
    """All Reddit traffic: enumerate IDs and fetch full submission trees."""

    def __init__(self, ratelimit_sleep: int = 2) -> None:
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT", "idea_scraper/0.1"),
        )
        self.ratelimit_sleep = ratelimit_sleep

    # ---------- 1a) list IDs inside [start_date, end_date] --------------- #
    def list_submission_ids(
        self,
        subreddit: str,
        start_date: str,
        end_date: str,
        *,
        min_score: Optional[int] = None,
        flairs: Optional[List[str]] = None,
    ) -> Iterator[Dict]:
        flair_set = {f.lower() for f in flairs} if flairs else None
        after = self._to_ts(start_date)
        before = self._to_ts(end_date) + 86_399  # include end-day

        sub_ref = self.reddit.subreddit(subreddit)

        for sub in sub_ref.new(limit=None):  # newest → oldest
            ts = int(sub.created_utc)
            if ts < after:
                break
            if ts > before:
                continue
            if min_score and sub.score < min_score:
                continue
            if flair_set and (sub.link_flair_text or "").lower() not in flair_set:
                continue
            yield {
                "id": sub.id,
                "created_utc": ts,
                "score": sub.score,
                "link_flair_text": sub.link_flair_text,
            }

    # ---------- 1b) NEW: search for submissions by keyword -------------- #
    def search_submissions(
        self,
        subreddit: str,
        keywords: List[str],
        time_filter: str = "all",
        *,
        min_score: Optional[int] = None,
        flairs: Optional[List[str]] = None,
    ) -> Iterator[Dict]:
        """
        Search for submissions using keywords.
        Note: Reddit's search uses `time_filter` ('year', 'month', etc.)
        instead of a precise date range.
        """
        query = " OR ".join(f'"{k}"' for k in keywords)
        flair_set = {f.lower() for f in flairs} if flairs else None
        sub_ref = self.reddit.subreddit(subreddit)

        for sub in sub_ref.search(query, sort="new", time_filter=time_filter):
            if min_score and sub.score < min_score:
                continue
            if flair_set and (sub.link_flair_text or "").lower() not in flair_set:
                continue
            yield {
                "id": sub.id,
                "created_utc": int(sub.created_utc),
                "score": sub.score,
                "link_flair_text": sub.link_flair_text,
            }

    # ---------- 2) fetch one submission plus ALL nested comments -------- #
    def fetch_submission_tree(self, submission_id: str) -> Dict:
        while True:  # retry on rate-limit / transient errors
            try:
                sub = self.reddit.submission(id=submission_id)
                sub.comments.replace_more(limit=None)
                return {
                    "submission": self._extract_submission(sub),
                    "comments": [self._extract_comment(c) for c in sub.comments],
                }
            except (
                APIException,
                RedditAPIException,
                RequestException,
                ResponseException,
                ServerError,
            ):
                time.sleep(self.ratelimit_sleep)
                continue

    # ---------- helpers -------------------------------------------------- #
    @staticmethod
    def _to_ts(iso: str) -> int:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    @staticmethod
    def _extract_submission(sub) -> Dict:
        return {
            "id": sub.id,
            "title": sub.title,
            "selftext": sub.selftext,
            "created_utc": int(sub.created_utc),
            "author": sub.author.name if sub.author else None,
            "score": sub.score,
            "num_comments": sub.num_comments,
            "link_flair_text": sub.link_flair_text,
            "url": sub.url,
            "permalink": sub.permalink,
        }

    @classmethod
    def _extract_comment(cls, c) -> Dict:
        """Recursively convert a PRAW Comment → dict, preserving thread structure."""
        return {
            "id": c.id,
            "parent_id": c.parent_id,
            "link_id": c.link_id,
            "author": c.author.name if c.author else None,
            "body": c.body,
            "created_utc": int(c.created_utc),
            "score": c.score,
            "depth": c.depth,
            "replies": [cls._extract_comment(r) for r in c.replies],
        }