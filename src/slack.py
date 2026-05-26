import logging
import os

import requests

logger = logging.getLogger(__name__)


def post_slack_message(message: str) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set; skipping Slack notification")
        return

    try:
        response = requests.post(
            webhook_url,
            json={"text": message},
            timeout=10,
        )
        response.raise_for_status()
    except Exception:
        logger.exception("Failed to post Slack notification")