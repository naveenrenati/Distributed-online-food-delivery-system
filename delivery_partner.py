"""
delivery_partner.py
-------------------
DeliveryPartner process (Process 3).

Does pre-trip checks BEFORE receiving any pickup request — these events
are concurrent with Restaurant and Customer's early events.
"""

import time
from process_base import ProcessBase

TRAVEL_TIME = 0.3   # seconds to simulate travel


class DeliveryPartner(ProcessBase):
    def __init__(self, pid: int, num_processes: int):
        super().__init__("DeliveryPartner", pid, num_processes)
        self.local_state["status"]    = "AVAILABLE"
        self.local_state["delivered"] = []

    def run(self):
        # Pre-trip checks — CONCURRENT with other processes
        self.internal_event("DeliveryPartner checks vehicle fuel & bag")
        self.internal_event("DeliveryPartner updates availability status in app")

        # Receive pickup request from OrderProcessor
        msg = self.receive(timeout=30)
        if not msg:
            return
        self.local_state["orders"].append("Order#1001")
        self.local_state["status"] = "EN_ROUTE_TO_RESTAURANT"

        # Travel to restaurant and pick up
        self.internal_event("Navigating to Restaurant for Order#1001")
        time.sleep(TRAVEL_TIME)
        self.internal_event("Arrived at Restaurant — picked up Order#1001")

        # Deliver to customer
        self.local_state["status"] = "EN_ROUTE_TO_CUSTOMER"
        time.sleep(TRAVEL_TIME)
        self.internal_event("Delivering Order#1001 to customer location")
        self.internal_event("Order Order#1001 handed over to customer")

        # Confirm delivery to OrderProcessor
        self.local_state["delivered"].append("Order#1001")
        self.local_state["status"] = "DELIVERED"
        self.send("OrderProcessor", "DELIVERED",
                  "Order#1001 — delivered at customer doorstep. Time: 35 min")
