import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> str:

        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(
                record,
                "service",
                "unknown",
            ),
            "event": getattr(
                record,
                "event",
                None,
            ),
            "event_id": getattr(
                record,
                "event_id",
                None,
            ),
            "correlation_id": getattr(
                record,
                "correlation_id",
                None,
            ),
            "message": record.getMessage(),
        }

        return json.dumps(log_data)


def setup_logger(service: str) -> logging.Logger:

    logger = logging.getLogger(service)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(
        JsonFormatter()
    )

    logger.addHandler(handler)

    logger.propagate = False

    return logger