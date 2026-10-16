"""
Contract for learning-platform events (S3 JSONL source).

No schema file ships with this source, so the contract is derived from the
fields every well-formed record carries. Any record with missing or extra
keys is schema drift (see silver.clean_events for file-level handling).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

EVENT_TYPES = (
    "enrollment",
    "module_started",
    "module_completed",
    "quiz_attempted",
    "quiz_passed",
    "course_completed",
    "certification_earned",
)
SCORED_EVENT_TYPES = frozenset({"quiz_attempted", "quiz_passed"})

# The expected key set of a raw JSON record; used for drift detection.
EXPECTED_EVENT_KEYS = frozenset(
    {"event_id", "employee_id", "event_date", "event_type", "course_name", "text", "score"}
)


class LearningEvent(BaseModel):
    """One learning activity event."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(pattern=r"^EV\d{10}$")
    employee_id: int = Field(gt=0)  # arrives as a string in JSON, e.g. "1178"
    event_date: datetime
    event_type: Literal[EVENT_TYPES]  # type: ignore[valid-type]
    course_name: str = Field(min_length=1)
    text: str = Field(min_length=1)
    score: int | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def score_matches_event_type(self) -> "LearningEvent":
        """Quiz events must carry a score; all other events must not."""
        if self.event_type in SCORED_EVENT_TYPES and self.score is None:
            raise ValueError(f"score is required for {self.event_type}")
        if self.event_type not in SCORED_EVENT_TYPES and self.score is not None:
            raise ValueError(f"score must be null for {self.event_type}")
        return self
