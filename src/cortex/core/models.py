from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

VALID_EVENT_TYPES = {
    "memory.created",
    "memory.updated",
    "memory.retracted",
    "memory.linked",
    "observation.recorded",
}

_REQUIRES_PARENT = {"memory.updated", "memory.retracted"}
_REQUIRES_ENTITY_TYPE = {"memory.created", "memory.updated", "observation.recorded"}


class Actor(BaseModel):
    runtime: str
    agent_id: str | None = None


class WriteMemoryInput(BaseModel):
    namespace: str
    event_type: str
    entity_type: str | None = None
    entity_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    actor: Actor
    parent_event_id: str | None = None
    metadata: dict[str, str] | None = None
    embedding: list[float] | None = None
    idempotency_key: str | None = None

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
        if self.event_type == "memory.linked":
            if "target_entity_id" not in self.payload:
                raise ValueError(
                    "payload must contain target_entity_id for memory.linked"
                )
        return self


def _clamp_limit(v: int) -> int:
    return min(v, 100)


class QueryMemoryInput(BaseModel):
    namespace: str
    entity_type: str | None = None
    time_after: str | None = None
    time_before: str | None = None
    metadata_filter: dict[str, str] | None = None
    embedding: list[float] | None = None
    limit: int = Field(default=10, ge=1)

    @model_validator(mode="after")
    def clamp_limit(self) -> QueryMemoryInput:
        self.limit = _clamp_limit(self.limit)
        return self


class GetRecentInput(BaseModel):
    namespace: str
    limit: int = Field(default=10, ge=1)

    @model_validator(mode="after")
    def clamp_limit(self) -> GetRecentInput:
        self.limit = _clamp_limit(self.limit)
        return self


class GetEntityInput(BaseModel):
    namespace: str
    entity_id: str


class InspectHistoryInput(BaseModel):
    namespace: str
    entity_id: str


class RebuildNamespaceInput(BaseModel):
    namespace: str


class WriteMemoryOutput(BaseModel):
    event_id: str
    sequence_number: int
    timestamp: str


class MemoryResult(BaseModel):
    entity_id: str
    entity_type: str
    namespace: str
    payload: dict[str, Any]
    actor: Actor
    timestamp: str
    metadata: dict[str, str] | None = None
    similarity_score: float | None = None


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
