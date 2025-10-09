"""
define canonical types
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict


class EVENTNAMES(str, Enum):
    """
    define canonical event names
    """

    ERROR = "error"
    CONFIG = "config"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event: EVENTNAMES
    ts: int  # Millis?
    run_id: int
    git_sha: str
    seed: int
    component: Any
