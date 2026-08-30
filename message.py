"""
message.py
----------
Two message types used between processes.

Message      — regular application message (ORDER, ASSIGN, READY, etc.)
MarkerMessage — special Chandy-Lamport snapshot control message
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class Message:
    sender:    str
    receiver:  str
    msg_type:  str        # e.g. 'ORDER', 'ASSIGN', 'READY', 'PICKUP', 'DELIVERED', 'CONFIRM'
    content:   str
    timestamp: List[int]  # vector clock at send time

    def __post_init__(self):
        self.timestamp = list(self.timestamp)  # always store a copy

    def __repr__(self):
        return f"[MSG] {self.sender}→{self.receiver} | {self.msg_type} | vc={self.timestamp} | \"{self.content}\""


@dataclass
class MarkerMessage:
    sender:      str
    receiver:    str
    snapshot_id: int
    msg_type:    str = field(default="MARKER", init=False)

    def __repr__(self):
        return f"[MARKER] {self.sender}→{self.receiver} | snap={self.snapshot_id}"
