"""Logging for tpuf_arrow.

The package logs to the "tpuf_arrow" logger and never configures handlers
itself. Records propagate to the root logger as usual, so nothing prints
unless your application has configured logging:

    logging.basicConfig()
    logging.getLogger("tpuf_arrow").setLevel(logging.INFO)

What each level carries:

    DEBUG    one line per request: kind, namespace, rows, timing, billed bytes.
             Includes rank_by and filters, which hold your users' search text.
    INFO     one line per scan, upsert, and save_log when it finishes.
    WARNING  one line per failed write request.

The API key is never logged. The turbopuffer SDK has its own logger
("turbopuffer", enabled with TURBOPUFFER_LOG=debug), and at DEBUG it logs
full request bodies, which includes every row you upsert.
"""

import logging

logger = logging.getLogger("tpuf_arrow")
logger.addHandler(logging.NullHandler())


def enable_logging(level: int | str = logging.INFO) -> None:
    """Print tpuf_arrow's log to stderr. Meant for scripts and notebooks.

    Applications with their own logging setup should configure the
    "tpuf_arrow" logger there instead.
    """
    if not any(getattr(h, "_tpuf_arrow", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        handler._tpuf_arrow = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    logger.setLevel(level)
