"""
restaurant.py
-------------
Restaurant process (Process 2).

Does kitchen prep BEFORE receiving any order — these events are
concurrent with Customer and DeliveryPartner's early events because
no messages have been exchanged yet at that point.
"""

import time
from process_base import ProcessBase

COOK_TIME = 0.3   # seconds to simulate cooking


class Restaurant(ProcessBase):
    def __init__(self, pid: int, num_processes: int):
        super().__init__("Restaurant", pid, num_processes)
        self.local_state["status"]   = "OPEN"
        self.local_state["prepared"] = []

    def run(self):
        # Pre-order kitchen prep — CONCURRENT with other processes
        self.internal_event("Restaurant opens kitchen, preheats oven")
        self.internal_event("Chef reviews today's menu items")

        # Receive order assignment from OrderProcessor
        msg = self.receive(timeout=10)
        if not msg:
            return
        self.local_state["orders"].append("Order#1001")
        self.local_state["status"] = "PREPARING"

        # Cook the food
        self.internal_event("Chef starts cooking for Order#1001")
        time.sleep(COOK_TIME)
        self.internal_event("Burger grilled, Fries fried for Order#1001")
        time.sleep(COOK_TIME * 0.7)
        self.local_state["prepared"].append("Order#1001")
        self.internal_event("Packaging complete for Order#1001")

        # Notify OrderProcessor that food is ready
        self.local_state["status"] = "READY"
        self.send("OrderProcessor", "READY",
                  "Order#1001 is packed and ready for pickup")

        # Wait for the snapshot marker before exiting
        # (OrderProcessor triggers snapshot right after receiving READY)
        self.snapshot_done_event.wait(timeout=5.0)
        self._drain_markers()
