"""
snapshot_collector.py
---------------------
Collects the global snapshot from all processes after Chandy-Lamport completes.

Consistency rule:
  For every in-transit message on channel (P → Q):
    P's vector clock at snapshot time must be >= the message's VC at P's own index.
    This confirms the message was actually sent before the snapshot was taken.
"""

from process_base import ProcessBase


def collect(processes: list) -> dict:
    """Return all process states and channel states from the snapshot."""
    return {
        "process_states": {p.name: p.snapshot_recorded_state
                           for p in processes if p.snapshot_recorded_state},
        "channel_states": {p.name: dict(p.channel_state) for p in processes},
    }


def check_consistency(snapshot: dict) -> tuple:
    """
    Returns (is_consistent: bool, explanation: str).

    For each in-transit message, verify the sender had already sent it
    (sender's VC at snapshot >= message's VC at sender's own index).
    """
    issues = []

    for receiver, channels in snapshot["channel_states"].items():
        for sender_name, msgs in channels.items():
            for msg in msgs:
                sender_state = snapshot["process_states"].get(sender_name)

                if not sender_state:
                    issues.append(
                        f"Message from {sender_name} found but {sender_name} "
                        f"has no recorded state."
                    )
                    continue

                sender_pid       = ProcessBase._registry[sender_name].pid
                snap_vc_at_sender = sender_state["vc"][sender_pid]
                msg_vc_at_sender  = msg.timestamp[sender_pid]

                if snap_vc_at_sender < msg_vc_at_sender:
                    issues.append(
                        f"In-transit msg ({sender_name}→{receiver}): "
                        f"sender VC[{sender_pid}]={snap_vc_at_sender} "
                        f"< msg VC[{sender_pid}]={msg_vc_at_sender} — not yet sent."
                    )

    if issues:
        return False, "INCONSISTENT:\n    " + "\n    ".join(issues)
    return True, (
        "All in-transit messages were sent before the snapshot. "
        "The captured global state is CONSISTENT."
        if any(msgs for ch in snapshot["channel_states"].values() for msgs in ch.values())
        else "No in-transit messages at snapshot time. All channels empty — trivially consistent."
    )


def print_snapshot_report(processes: list):
    snapshot = collect(processes)

    print("\n" + "=" * 70)
    print("  GLOBAL SNAPSHOT REPORT  (Chandy-Lamport Algorithm)")
    print("=" * 70)

    # Process states
    print("\n  PROCESS STATES AT SNAPSHOT TIME:")
    print("  " + "-" * 66)
    if not snapshot["process_states"]:
        print("  (no states recorded — snapshot may not have completed)")
    else:
        for name, state in snapshot["process_states"].items():
            ls = state.get("local_state", {})
            print(f"\n  Process : {name}")
            print(f"  VC      : {state['vc']}")
            print(f"  Status  : {ls.get('status', 'N/A')}")
            print(f"  Orders  : {ls.get('orders', [])}")
            for k, v in ls.items():
                if k not in ("status", "orders"):
                    print(f"  {k:12s}: {v}")

    # Channel states
    print("\n\n  CHANNEL STATES AT SNAPSHOT TIME (in-transit messages):")
    print("  " + "-" * 66)
    any_in_transit = False
    for receiver, channels in snapshot["channel_states"].items():
        for sender, msgs in channels.items():
            label = f"\n  Channel {sender} → {receiver}:"
            if msgs:
                any_in_transit = True
                print(label)
                for m in msgs:
                    print(f"    {m}")
            else:
                print(f"{label}  (empty)")

    if not any_in_transit:
        print("\n  All channels empty at snapshot time.")

    # Consistency result
    ok, explanation = check_consistency(snapshot)
    print("\n\n  CONSISTENCY CHECK:")
    print("  " + "-" * 66)
    print(f"  Result  : {'✓ CONSISTENT' if ok else '✗ INCONSISTENT'}")
    print(f"  Details : {explanation}")
    print("\n" + "=" * 70)
