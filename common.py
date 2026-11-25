import logging
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("cinema")


def enabled(name):
    return os.getenv(name, "true").lower() in {"1", "true", "yes"}


def retry(action, errors, attempts=3):
    for attempt in range(attempts):
        try:
            return action()
        except errors as exc:
            if attempt + 1 == attempts:
                raise
            delay = min(0.5 * 2 ** attempt, 5)
            log.warning("%s: %s, retry in %.1fs", action.__name__, exc, delay)
            time.sleep(delay)
