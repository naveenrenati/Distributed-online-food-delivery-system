"""
vector_clock.py
---------------
Vector Clock for distributed event timestamping.

Rules:
  internal_event : clock[own] += 1
  send_event     : clock[own] += 1  (same rule — attach result to message)
  receive_event  : clock = max(clock, received) element-wise, then clock[own] += 1
"""


class VectorClock:
    def __init__(self, process_id: int, num_processes: int):
        self.pid = process_id
        self.clock = [0] * num_processes

    # ── Update rules ──────────────────────────────────────────────────

    def tick(self) -> list:
        """Increment own component. Used for both internal and send events."""
        self.clock[self.pid] += 1
        return self.get()

    # Aliases so process code reads naturally
    def internal_event(self) -> list:
        return self.tick()

    def send_event(self) -> list:
        return self.tick()

    def receive_event(self, received: list) -> list:
        """Merge received clock then tick own component."""
        for i in range(len(self.clock)):
            self.clock[i] = max(self.clock[i], received[i])
        self.clock[self.pid] += 1
        return self.get()

    # ── Helpers ───────────────────────────────────────────────────────

    def get(self) -> list:
        return list(self.clock)

    def __str__(self):
        return str(self.clock)

    # ── Comparison ────────────────────────────────────────────────────

    @staticmethod
    def happened_before(a: list, b: list) -> bool:
        """True if a → b  (a happened-before b)."""
        return all(x <= y for x, y in zip(a, b)) and any(x < y for x, y in zip(a, b))

    @staticmethod
    def concurrent(a: list, b: list) -> bool:
        """True if neither a→b nor b→a."""
        hb = VectorClock.happened_before
        return not hb(a, b) and not hb(b, a)
