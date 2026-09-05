import logging
import re

# Patterns to redact from logs - NEVER log secrets or full webhook URLs
REDACT_PATTERNS = [
    (re.compile(r"discord\.com/api/webhooks/[^\s\"']+"), "discord.com/api/webhooks/***REDACTED***"),
    (re.compile(r"postgresql\+asyncpg://[^\s]+"), "postgresql+asyncpg://***REDACTED***"),
    (re.compile(r"DATABASE_URL[^\n]+"), "DATABASE_URL=***REDACTED***"),
    (re.compile(r"APP_SECRET_KEY[^\n]+"), "APP_SECRET_KEY=***REDACTED***"),
]

class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        for pat, repl in REDACT_PATTERNS:
            msg = pat.sub(repl, msg)
        return msg

def setup_logging(level: str = "INFO"):
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # quiet noisy libs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
