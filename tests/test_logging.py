import logging

from cortex.core.logging import configure_logging, get_logger


def test_get_logger_returns_logger():
    logger = get_logger("cortex.test")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "cortex.test"


def test_configure_logging_sets_root_level():
    configure_logging("DEBUG")
    assert logging.getLogger().level == logging.DEBUG


def test_configure_logging_info_level():
    configure_logging("INFO")
    assert logging.getLogger().level == logging.INFO


def test_get_logger_different_names():
    a = get_logger("cortex.journal")
    b = get_logger("cortex.projections")
    assert a.name != b.name
    assert a is not b
