"""Stub the optional `cronkite` package so unit tests run without it installed."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

# Insert a lightweight stub before any test module imports generate_stories.
if "cronkite" not in sys.modules:
    stub = ModuleType("cronkite")
    stub.Cronkite = MagicMock()
    stub.CronkiteConfig = MagicMock()
    sys.modules["cronkite"] = stub
