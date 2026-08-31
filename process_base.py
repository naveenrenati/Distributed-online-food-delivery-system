"""
process_base.py
---------------
Base class shared by all 4 processes.

Responsibilities:
  - Vector clock updates on every event
  - Message sending and receiving
  - Colour-coded event logging
  - Chandy-Lamport snapshot participation

Inbox design:
  Two separate queues — markers always drain before regular messages
  so Chandy-Lamport sees MARKERs at the right time regardless of thread speed.
"""

import queue
import threading
from vector_clock import VectorClock
from message import Message, MarkerMessage

# Colour codes — one per process for readable terminal output
COLOURS = {
    "Customer":        "\033[94m",   # blue
    "OrderProcessor":  "\033[92m",   # green
    "Restaurant":      "\033[93m",   # yellow
    "DeliveryPartner": "\033[95m",   # magenta
    "RESET":           "\033[0m",
}


class ProcessBase:
    # All processes register here so they can find each other by name
    _registry: dict = {}

    def __init__(self, name: str, pid: int, num_processes: int):
        self.name = name
        self.pid  = pid
        self.vc   = VectorClock(pid, num_processes)

        # Two-queue inbox: markers are high-priority
        self._marker_q: queue.Queue = queue.Queue()
        self._msg_q:    queue.Queue = queue.Queue()

        # Local application state — subclasses add their own fields
        self.local_state: dict = {"status": "IDLE", "orders": []}

        # Snapshot state
        self.snapshot_active        = False
        self.snapshot_recorded_state: dict = {}
        self.channel_state:          dict = {}   # {sender_name: [in-transit messages]}
        self.pending_marker_channels: set = set()
        self._snap_lock              = threading.Lock()
        self.snapshot_done_event     = threading.Event()

        # Event log — collected after run for analysis
        self.event_log: list = []

        ProcessBase._registry[name] = self

    # ── Logging ───────────────────────────────────────────────────────

    def _fmt(self, text: str) -> str:
        c = COLOURS.get(self.name, "")
        return f"{c}{text}{COLOURS['RESET']}"

    def log(self, description: str, vc: list):
        line = f"[{self.name:16s}] {description:55s} VC={vc}"
        print(self._fmt(line))
        self.event_log.append({"process": self.name, "event": description, "vc": list(vc)})

    # ── Communication ─────────────────────────────────────────────────

    def send(self, target: str, msg_type: str, content: str) -> Message:
        """Send event: tick own clock, build message, deliver to target."""
        ts  = self.vc.send_event()
        msg = Message(self.name, target, msg_type, content, ts)
        self.log(f"SEND → {target} | {msg_type}: {content}", ts)
        ProcessBase._registry[target]._msg_q.put(msg)
        return msg

    def receive(self, timeout: float = 10.0) -> "Message | None":
        """
        Block until a regular message arrives.
        Drains all pending MARKERs before returning each message —
        this is what keeps snapshot ordering correct.
        """
        import time
        deadline = time.monotonic() + timeout

        while True:
            self._drain_markers()

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.log("TIMEOUT — no message received", self.vc.get())
                return None

            try:
                msg = self._msg_q.get(timeout=min(0.05, remaining))
            except queue.Empty:
                continue

            # If snapshot is active, record this message as in-transit
            # on the channel it came from (until that channel's Marker arrives)
            with self._snap_lock:
                if (self.snapshot_active
                        and msg.sender in self.channel_state
                        and msg.sender in self.pending_marker_channels):
                    self.channel_state[msg.sender].append(msg)

            ts = self.vc.receive_event(msg.timestamp)
            self.log(f"RECV ← {msg.sender} | {msg.msg_type}: {msg.content}", ts)
            return msg

    def internal_event(self, description: str) -> list:
        """Internal event: drain markers first, then tick own clock."""
        self._drain_markers()
        ts = self.vc.internal_event()
        self.log(f"INTERNAL | {description}", ts)
        return ts

    # ── Marker handling (Chandy-Lamport) ──────────────────────────────

    def _drain_markers(self):
        """Process all waiting MARKERs immediately (non-blocking)."""
        while not self._marker_q.empty():
            try:
                self._handle_marker(self._marker_q.get_nowait())
            except queue.Empty:
                break

    def initiate_snapshot(self, snapshot_id: int = 1):
        """
        Called by the process that STARTS the snapshot.
        Records own state, then sends MARKERs to all others.
        """
        with self._snap_lock:
            self._record_state(snapshot_id)
            for name, proc in ProcessBase._registry.items():
                if name != self.name:
                    self.pending_marker_channels.add(name)
                    self.channel_state[name] = []
                    marker = MarkerMessage(self.name, name, snapshot_id)
                    print(self._fmt(f"[{self.name:16s}] SNAPSHOT INIT — sending {marker}"))
                    proc._marker_q.put(marker)

    def _handle_marker(self, marker: MarkerMessage):
        """
        Chandy-Lamport marker rules:
          First marker  → record state, forward marker to all others, ACK initiator
          Later markers → close that channel (stop recording it)
        """
        with self._snap_lock:
            if not self.snapshot_active:
                # First marker — record own state immediately
                self._record_state(marker.snapshot_id)

                # Close the incoming channel (no messages in-transit before this marker)
                self.channel_state[marker.sender] = []

                # Forward to all others except the sender
                for name, proc in ProcessBase._registry.items():
                    if name != self.name and name != marker.sender:
                        self.pending_marker_channels.add(name)
                        self.channel_state[name] = []
                        fwd = MarkerMessage(self.name, name, marker.snapshot_id)
                        print(self._fmt(f"[{self.name:16s}] MARKER FWD → {name}"))
                        proc._marker_q.put(fwd)

                # ACK back to initiator so it can close its outgoing channel to us
                initiator = ProcessBase._registry.get(marker.sender)
                if initiator:
                    ack = MarkerMessage(self.name, marker.sender, marker.snapshot_id)
                    print(self._fmt(f"[{self.name:16s}] MARKER ACK → {marker.sender}"))
                    initiator._marker_q.put(ack)

            else:
                # Subsequent marker — close this channel
                self.pending_marker_channels.discard(marker.sender)
                in_transit = self.channel_state.get(marker.sender, [])
                print(self._fmt(
                    f"[{self.name:16s}] MARKER ← {marker.sender} "
                    f"— channel closed | {len(in_transit)} in-transit msg(s): {in_transit}"
                ))

            # If all channels are closed, this process's snapshot is done
            if not self.pending_marker_channels and self.snapshot_active:
                self.snapshot_done_event.set()

    def _record_state(self, snapshot_id: int):
        """Save current local state — called the moment first MARKER is seen."""
        self.snapshot_active = True
        self.snapshot_recorded_state = {
            "snapshot_id": snapshot_id,
            "process":     self.name,
            "vc":          self.vc.get(),
            "local_state": {k: list(v) if isinstance(v, list) else v
                            for k, v in self.local_state.items()},
        }
        print(self._fmt(
            f"[{self.name:16s}] *** STATE RECORDED *** "
            f"vc={self.vc.get()} | {self.local_state}"
        ))
