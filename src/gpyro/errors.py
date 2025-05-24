from typing import Self


class UnableToReachURL(Exception):
    def __init__(self: Self, message: str, errors=None) -> None:
        super().__init__(message)
        self.errors = errors
