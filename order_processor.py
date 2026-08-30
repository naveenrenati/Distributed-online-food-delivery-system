"""
order_processor.py
------------------
OrderProcessor process (Process 1).
Central coordinator — receives orders, assigns to restaurant,
dispatches delivery partner, and INITIATES the global snapshot.
"""

from process_base import ProcessBase


class OrderProcessor(ProcessBase):
    def __init__(self, pid: int, num_processes: int):
        super().__init__("OrderProcessor", pid, num_processes)
        self.local_state["status"]   = "IDLE"
        self.local_state["assigned"] = []

    def run(self):
        # Step 1 — receive order from Customer
        msg = self.receive(timeout=10)
        if not msg:
            return
        self.local_state["orders"].append("Order#1001")
        self.local_state["status"] = "PROCESSING"

        # Step 2 — validate payment
        self.internal_event("Validating Order#1001 — payment confirmed")

        # Step 3 — assign order to Restaurant
        self.local_state["assigned"].append("Order#1001")
        self.send("Restaurant", "ASSIGN",
                  "Order#1001 — Burger Meal x1, Fries x1 — Prep time: 15 min")

        # Step 4 — wait for food ready signal
        msg = self.receive(timeout=15)
        if not msg:
            return

        # Step 5 — take snapshot now (food is ready, all processes active)
        self.initiate_snapshot(snapshot_id=1)

        # Step 6 — dispatch delivery partner
        self.local_state["status"] = "DISPATCHING"
        self.send("DeliveryPartner", "PICKUP",
                  "Order#1001 ready at Restaurant — pickup location: Gate 2")

        # Step 7 — wait for delivery confirmation
        msg = self.receive(timeout=30)
        if not msg:
            return

        # Step 8 — forward confirmation to customer
        self.local_state["status"] = "DELIVERED"
        self.send("Customer", "CONFIRM",
                  "Order#1001 delivered successfully by DeliveryPartner")
