import pytest

from cortex.core.errors import (
    CortexError,
    NotFoundError,
    InvalidEventTypeError,
    ValidationError,
    IdempotencyConflictError,
    InvalidEmbeddingError,
    RebuildInProgressError,
    RebuildFailedError,
    VALID_ERROR_CODES,
)


class TestCortexErrorSubclasses:
    def test_not_found_error(self):
        err = NotFoundError("Entity xyz not found")
        assert isinstance(err, CortexError)
        assert isinstance(err, Exception)
        assert err.code == "not_found"
        assert err.message == "Entity xyz not found"
        assert err.to_dict() == {"error": "not_found", "message": "Entity xyz not found"}

    def test_invalid_event_type_error(self):
        err = InvalidEventTypeError("bad type")
        assert isinstance(err, CortexError)
        assert err.code == "invalid_event_type"
        assert err.to_dict() == {"error": "invalid_event_type", "message": "bad type"}

    def test_validation_error(self):
        err = ValidationError("Missing field x")
        assert err.code == "validation_error"
        assert err.to_dict() == {"error": "validation_error", "message": "Missing field x"}

    def test_idempotency_conflict_error(self):
        err = IdempotencyConflictError("key clash")
        assert err.code == "idempotency_conflict"

    def test_invalid_embedding_error(self):
        err = InvalidEmbeddingError("wrong dims")
        assert err.code == "invalid_embedding"

    def test_rebuild_in_progress_error(self):
        err = RebuildInProgressError("rebuild running")
        assert err.code == "rebuild_in_progress"

    def test_rebuild_failed_error(self):
        err = RebuildFailedError("replay crashed")
        assert err.code == "rebuild_failed"

    def test_all_subclasses_are_catchable_as_cortex_error(self):
        errors = [
            NotFoundError("x"),
            InvalidEventTypeError("x"),
            ValidationError("x"),
            IdempotencyConflictError("x"),
            InvalidEmbeddingError("x"),
            RebuildInProgressError("x"),
            RebuildFailedError("x"),
        ]
        for err in errors:
            assert isinstance(err, CortexError), f"{type(err)} is not a CortexError"
            assert isinstance(err, Exception)

    def test_to_dict_return_type(self):
        """to_dict must return dict[str, str]."""
        d = NotFoundError("test").to_dict()
        assert isinstance(d, dict)
        for k, v in d.items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def test_str_representation(self):
        err = NotFoundError("Entity xyz not found")
        assert "Entity xyz not found" in str(err)


def test_known_error_codes():
    expected = {
        "not_found",
        "invalid_event_type",
        "validation_error",
        "idempotency_conflict",
        "invalid_embedding",
        "rebuild_in_progress",
        "rebuild_failed",
    }
    assert VALID_ERROR_CODES == expected
    assert isinstance(VALID_ERROR_CODES, frozenset)
