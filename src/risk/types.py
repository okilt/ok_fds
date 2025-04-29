# guidline_types.py (or similar)
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date  # Also used by Event class
from enum import Enum, auto
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)  # Get logger in each module


@dataclass
class ActionSuggestion:
    """Represents a suggested action based on a guideline check."""

    message: str
    severity: str = "Info"  # e.g., Info, Warning, Error, Critical
    details: Dict[str, Any] = field(
        default_factory=dict
    )  # e.g., {'quantity': 10, 'type': 'buy', 'ticker': 'AAPL', 'value_change': 1750.0}

    def __repr__(self):
        return f"Suggestion(message='{self.message}', severity='{self.severity}', details={self.details})"


@dataclass
class CheckDetails:
    """Holds the basic results of a pure check operation."""

    is_breached: bool
    actual_value: Optional[Any]
    target_value: Optional[Any]
    lower_limit: Optional[Any]
    upper_limit: Optional[Any]
    # Could add more details like check description or key values used in calculation
    check_description: Optional[str] = None
    error_message: Optional[str] = None  # Check-specific errors


@dataclass
class GuidelineResult:
    """Final result object for a guideline check."""

    guideline_id: uuid.UUID  # UUID of the guideline being referenced
    guideline_name: str  # Human-readable name
    guideline_category: "GuidelineCategory"  # Category Enum (defined below)
    is_breached: bool
    actual_value: Optional[Any]
    target_value: Optional[Any]
    lower_limit: Optional[Any]
    upper_limit: Optional[Any]
    suggestion: Optional[ActionSuggestion]
    # May hold key context info instead of all check data
    # check_context: Dict[str, Any] = field(default_factory=dict)
    raw_check_value: Optional[Any] = (
        None  # Optional: value before processing (e.g., for context)
    )
    error_message: Optional[str] = None  # Errors during the entire process

    def __repr__(self):
        status = "BREACHED" if self.is_breached else "OK"
        if self.error_message:
            status = f"ERROR ({self.error_message})"
        # Adjust value formatting for display
        actual_str = (
            f"{self.actual_value:.4f}"
            if isinstance(self.actual_value, float)
            else str(self.actual_value)
        )

        return (
            f"Result(id={str(self.guideline_id)[:8]}, name='{self.guideline_name}', "
            f"status={status}, actual={actual_str}, target={self.target_value}, "
            f"suggestion={self.suggestion})"
        )


# --- Guideline Category Enum ---
class GuidelineCategory(Enum):
    REGULATORY = auto()
    INVESTMENT_STRATEGY = auto()
    OPERATIONAL = auto()
    RISK = auto()
    DATA_QUALITY = auto()  # e.g., for data quality checks


# --- Example Position Data Structure (used by Calculators) ---
@dataclass
class Position:
    """Simple example representing a position in a portfolio."""

    ticker: str
    quantity: float
    # Other attributes (e.g., market_value, asset_class, sector, country)
    # are assumed to be in the resolved data dictionary from the DependencyResolver
    # or the Resolver could return a list of enriched Position objects.


# === Event System Types ===


@dataclass
class EventScheduleResult:
    """Result object for event schedule generation."""

    event_id: uuid.UUID
    event_name: str
    event_category: "EventCategory"  # Optional: Event category Enum (defined below)
    schedule_dates: List[date] = field(default_factory=list)
    error_message: Optional[str] = None

    def __repr__(self):
        status = "OK" if not self.error_message else f"ERROR ({self.error_message})"
        return (
            f"EventResult(id={str(self.event_id)[:8]}, name='{self.event_name}', "
            f"status={status}, schedule_count={len(self.schedule_dates)})"
        )


# (Optional) Event Category Enum
class EventCategory(Enum):
    REPORTING = auto()
    MEETING = auto()
    TRADING_WINDOW = auto()
    SYSTEM_TASK = auto()
    # ... other categories
