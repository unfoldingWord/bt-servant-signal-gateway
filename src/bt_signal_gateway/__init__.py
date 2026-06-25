"""BT Servant Signal gateway.

A thin relay between Signal (via a local signal-cli daemon) and the
bt-servant-worker. This package does NO AI processing itself; all "brains"
live in the worker. See the architecture notes in CLAUDE.md.
"""

__all__ = ["__version__"]

__version__ = "0.7.1"
