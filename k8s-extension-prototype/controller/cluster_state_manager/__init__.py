"""Cluster state manager components.

This package matches the system-design component boundary: observation produces
point-in-time cluster snapshots, while the physical GPU registry preserves
durable GPU identity and queue state across planning epochs.
"""

