from typing import ClassVar


class CortexError(Exception):
    code: ClassVar[str]

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, str]:
        return {"error": self.code, "message": self.message}


class NotFoundError(CortexError):
    code = "not_found"


class InvalidEventTypeError(CortexError):
    code = "invalid_event_type"


class ValidationError(CortexError):
    code = "validation_error"


class IdempotencyConflictError(CortexError):
    code = "idempotency_conflict"


class InvalidEmbeddingError(CortexError):
    code = "invalid_embedding"


class RebuildInProgressError(CortexError):
    code = "rebuild_in_progress"


class RebuildFailedError(CortexError):
    code = "rebuild_failed"


VALID_ERROR_CODES: frozenset[str] = frozenset({
    cls.code for cls in CortexError.__subclasses__()
})
