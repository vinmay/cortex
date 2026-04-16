from cortex.core.errors import CortexError


def test_cortex_error_to_dict():
    err = CortexError("not_found", "Entity xyz not found")
    assert err.to_dict() == {
        "error": "not_found",
        "message": "Entity xyz not found",
    }


def test_cortex_error_is_exception():
    err = CortexError("validation_error", "Missing field")
    assert isinstance(err, Exception)
    assert str(err) == "validation_error: Missing field"


def test_known_error_codes():
    from cortex.core.errors import VALID_ERROR_CODES

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
