from __future__ import annotations

import json
import logging
from typing import Any
from urllib import request

from market_adaptive.config import DiscordNotificationConfig

logger = logging.getLogger(__name__)


class DiscordNotifier:
    def __init__(self, config: DiscordNotificationConfig) -> None:
        self.config = config

    @property
    def enabled(self) -> bool:
        return self.config.enabled and bool(self.config.webhook_url or (self.config.bot_token and self.config.channel_id))

    def send(self, title: str, message: str) -> bool:
        if not self.enabled:
            return False

        content = f"**{title}**\n{message}"
        if self.config.webhook_url:
            return self._send_via_webhook(content)
        return self._send_via_bot(content)

    def _send_via_webhook(self, content: str) -> bool:
        payload = {
            "username": self.config.username,
            "content": content,
        }
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            self.config.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=10) as response:
            return 200 <= response.status < 300

    def _send_via_bot(self, content: str) -> bool:
        if not self.config.channel_id or not self.config.bot_token:
            return False
        payload = {"content": content}
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"https://discord.com/api/v10/channels/{self.config.channel_id}/messages",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bot {self.config.bot_token}",
            },
            method="POST",
        )
        with request.urlopen(req, timeout=10) as response:
            return 200 <= response.status < 300


class NullNotifier:
    def send(self, title: str, message: str) -> bool:
        logger.debug("Notification skipped: %s | %s", title, message)
        return False
