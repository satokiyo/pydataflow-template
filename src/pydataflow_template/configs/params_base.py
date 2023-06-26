from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Query:
    """
    Represents a SQL query.
    """

    sql: str = ""
