import logging
import os

import requests

logger = logging.getLogger(__name__)


def post_slack_message(text: str) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logger.warning("Slack webhook not configured; skipping Slack message")
        return

    try:
        response = requests.post(
            webhook_url,
            json={"text": text},
            timeout=10,
        )
        response.raise_for_status()
    except Exception:
        logger.exception("Failed to post Slack notification")