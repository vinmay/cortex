from __future__ import annotations

import math
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from cortex.core.timestamps import canonicalize

VALID_EVENT_TYPES: frozenset[str] = frozenset({
    "memory.created",
    "memory.updated",
    "memory.retracted",
    "memory.linked",
    "observation.recorded",
})

_REQUIRES_PARENT = {"memory.updated", "memory.retracted"}
_REQUIRES_ENTITY_TYPE = {"memory.created", "memory.updated", "observation.recorded"}
_REQUIRES_NON_EMPTY_PAYLOAD = {"memory.created", "memory.updated", "observation.recorded"}

# C2: metadata key validation pattern
_METADATA_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_METADATA_KEY_MAX_LEN = 64


def _validate_metadata_keys(metadata: dict[str, str] | None) -> dict[str, str] | None:
    """Validate all metadata keys match the allowed pattern."""
    if metadata is None:
        return None
    for key in metadata:
        if len(key) > _METADATA_KEY_MAX_LEN:
            raise ValueError(
                f"Metadata key {key!r} exceeds maximum length of {_METADATA_KEY_MAX_LEN}"
            )
        if not _METADATA_KEY_PATTERN.match(key):
            raise ValueError(
                f"Metadata key {key!r} is invalid — keys must match "
                r"^[A-Za-z_][A-Za-z0-9_]*$ (no dots, dashes, or special chars)"
            )
    return metadata


class Actor(BaseModel):
    model_config = {"extra": "forbid"}

    runtime: str = Field(min_length=1, max_length=256)
    agent_id: str | None = Field(default=None, min_length=1)


class WriteMemoryInput(BaseModel):
    model_config = {"extra": "forbid"}

    namespace: str = Field(min_length=1, max_length=256)
    event_type: str
    entity_type: str | None = Field(default=None, min_length=1, max_length=256)
    entity_id: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any] = Field(default_factory=dict)
    actor: Actor
    parent_event_id: str | None = None
    metadata: dict[str, str] | None = None
    embedding: list[float] | None = None
    idempotency_key: str | None = None

    @field_validator("metadata", mode="after")
    @classmethod
    def validate_metadata_keys(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        return _validate_metadata_keys(v)

    @field_validator("embedding", mode="after")
    @classmethod
    def validate_embedding(cls, v: list[float] | None) -> list[float] | None:
        if v is None:
            return None
        if len(v) == 0:
            raise ValueError("embedding must not be empty")
        if len(v) > 4096:
            raise ValueError(
                f"embedding has {len(v)} dimensions; maximum allowed is 4096"
            )
        for i, val in enumerate(v):
            if not math.isfinite(val):
                raise ValueError(
                    f"embedding[{i}] is not finite ({val!r}); "
                    "all embedding values must be finite floats"
                )
        return v

    @model_validator(mode="after")
    def validate_event_rules(self) -> WriteMemoryInput:
        if self.event_type not in VALID_EVENT_TYPES:
            raise ValueError(
                f"event_type must be one of {sorted(VALID_EVENT_TYPES)}"
            )
        if self.event_type in _REQUIRES_PARENT and not self.parent_event_id:
            raise ValueError(
                f"parent_event_id is required for {self.event_type}"
            )
        if self.event_type in _REQUIRES_ENTITY_TYPE and not self.entity_type:
            raise ValueError(
                f"entity_type is required for {self.event_type}"
            )
        # H9: non-empty payload for certain event types
        if self.event_type in _REQUIRES_NON_EMPTY_PAYLOAD and not self.payload:
            raise ValueError(
                f"payload must not be empty for {self.event_type}"
            )
        if self.event_type == "memory.linked":
            if "target_entity_id" not in self.payload:
                raise ValueError(
                    "payload must contain target_entity_id for memory.linked"
                )
            if not isinstance(self.payload["target_entity_id"], str) or not self.payload["target_entity_id"]:
                raise ValueError(
                    "payload['target_entity_id'] must be a non-empty string for memory.linked"
                )
        return self


class QueryMemoryInput(BaseModel):
    model_config = {"extra": "forbid"}

    namespace: str = Field(min_length=1, max_length=256)
    entity_type: str | None = None
    time_after: str | None = None
    time_before: str | None = None
    metadata_filter: dict[str, str] | None = None
    embedding: list[float] | None = None
    # H4: reject > 100 instead of silently clamping
    limit: int = Field(default=10, ge=1, le=100)

    @field_validator("metadata_filter", mode="after")
    @classmethod
    def validate_metadata_filter_keys(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        return _validate_metadata_keys(v)

    @field_validator("time_after", "time_before", mode="before")
    @classmethod
    def canonicalize_time(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return canonicalize(v)


class GetRecentInput(BaseModel):
    model_config = {"extra": "forbid"}

    namespace: str = Field(min_length=1, max_length=256)
    # H4: reject > 100 instead of silently clamping
    limit: int = Field(default=10, ge=1, le=100)


class GetEntityInput(BaseModel):
    model_config = {"extra": "forbid"}

    namespace: str = Field(min_length=1, max_length=256)
    entity_id: str = Field(min_length=1, max_length=256)


class InspectHistoryInput(BaseModel):
    model_config = {"extra": "forbid"}

    namespace: str = Field(min_length=1, max_length=256)
    entity_id: str = Field(min_length=1, max_length=256)


class RebuildNamespaceInput(BaseModel):
    model_config = {"extra": "forbid"}

    namespace: str = Field(min_length=1, max_length=256)


class WriteMemoryOutput(BaseModel):
    event_id: str
    sequence_number: int
    timestamp: str

    @field_validator("timestamp", mode="before")
    @classmethod
    def canonicalize_timestamp(cls, v: str) -> str:
        return canonicalize(v)


class MemoryResult(BaseModel):
    entity_id: str
    entity_type: str
    namespace: str
    payload: dict[str, Any]
    actor: Actor
    timestamp: str
    metadata: dict[str, str] | None = None
    similarity_score: float | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def canonicalize_timestamp(cls, v: str) -> str:
        return canonicalize(v)


class QueryMemoryOutput(BaseModel):
    results: list[MemoryResult]
    count: int


class HistoryEvent(BaseModel):
    sequence_number: int
    event_id: str
    event_type: str
    entity_type: str | None
    entity_id: str
    payload: dict[str, Any]
    actor: Actor
    metadata: dict[str, str] | None
    parent_event_id: str | None
    schema_version: int
    timestamp: str

    @field_validator("timestamp", mode="before")
    @classmethod
    def canonicalize_timestamp(cls, v: str) -> str:
        return canonicalize(v)


class InspectHistoryOutput(BaseModel):
    events: list[HistoryEvent]


class RebuildOutput(BaseModel):
    events_replayed: int
    duration_ms: int


class HealthOutput(BaseModel):
    status: str
    event_count: int
    namespace_count: int
    last_write_timestamp: str | None
    embedding_dimension: int | None
