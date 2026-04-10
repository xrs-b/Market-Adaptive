from __future__ import annotations


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def send(self, title: str, message: str) -> bool:
        self.messages.append((title, message))
        return True
