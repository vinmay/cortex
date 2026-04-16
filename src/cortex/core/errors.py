VALID_ERROR_CODES = {
    "not_found",
    "invalid_event_type",
    "validation_error",
    "idempotency_conflict",
    "invalid_embedding",
    "rebuild_in_progress",
    "rebuild_failed",
}


class CortexError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message}
