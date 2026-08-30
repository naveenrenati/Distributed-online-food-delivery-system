"""
customer.py
-----------
Customer process (Process 0).
Places a food order and waits for delivery confirmation.
"""

from process_base import ProcessBase


class Customer(ProcessBase):
    def __init__(self, pid: int, num_processes: int):
        super().__init__("Customer", pid, num_processes)
        self.local_state["status"] = "WAITING_TO_ORDER"

    def run(self):
        # Decide what to order (internal event)
        self.local_state["status"] = "DECIDING"
        self.internal_event("Customer decides to order a Burger Meal")

        # Place the order
        self.local_state["status"] = "ORDER_PLACED"
        self.local_state["orders"].append("Burger Meal")
        self.send("OrderProcessor", "ORDER", "Order#1001 — Burger Meal x1, Fries x1")

        # Wait for delivery confirmation
        msg = self.receive(timeout=30)
        if msg:
            self.local_state["status"] = "ORDER_DELIVERED"
            self.internal_event(f"Order delivered! '{msg.content}'")
        else:
            self.local_state["status"] = "TIMEOUT"
