# checkers.py
import logging
import math  # Import math for NaN check
from typing import Any, Dict, Optional

from .interfaces import IChecker
from .types import CheckDetails

log = logging.getLogger(__name__)


class FloatRangeChecker(IChecker):
    """Checks if a numeric value falls within a specified range [lower, upper]."""

    def check(
        self, value_to_check: Optional[Any], params: Dict[str, Any]
    ) -> CheckDetails:
        actual = value_to_check
        target = params.get("target")  # Optional: for information/suggestion context
        lower = params.get("lower_limit")
        upper = params.get("upper_limit")
        error_msg = None
        is_breached = False

        log.debug(
            f"Checking value '{actual}' against range [{lower}, {upper}] (Target: {target})"
        )

        # Check if value exists and is numeric
        if actual is None:
            error_msg = "Value to check is None"
            is_breached = True  # Treat None as breach (configurable?)
        elif isinstance(
            actual, bool
        ):  # Explicitly handle bools if they shouldn't be treated as 0/1
            error_msg = f"Value is a boolean, not numeric: {actual}"
            is_breached = True
        elif not isinstance(actual, (int, float)) or math.isnan(
            actual
        ):  # Check for numeric types and NaN
            error_msg = (
                f"Value is not a valid number: {actual} (type: {type(actual).__name__})"
            )
            is_breached = True
            if math.isnan(actual):
                error_msg = "Value is NaN"

        # Only proceed if value is valid so far
        if not is_breached and error_msg is None:
            # Lower bound check (if lower limit is specified)
            if lower is not None:
                try:
                    lower_f = float(lower)
                    if actual < lower_f:
                        is_breached = True
                        log.info(
                            f"Breached lower limit ({lower_f}) with value {actual}"
                        )
                except (ValueError, TypeError):
                    error_msg = f"Invalid lower_limit parameter: {lower}"
                    is_breached = True

            # Upper bound check (if upper limit is specified and no breach/error yet)
            if not is_breached and upper is not None:
                try:
                    upper_f = float(upper)
                    if actual > upper_f:
                        is_breached = True
                        log.info(
                            f"Breached upper limit ({upper_f}) with value {actual}"
                        )
                except (ValueError, TypeError):
                    error_msg = f"Invalid upper_limit parameter: {upper}"
                    is_breached = True

        # Optional: Validate target type if present
        if target is not None and not isinstance(target, (int, float)):
            log.warning(f"Non-numeric target value provided: {target}")
            # error_msg = error_msg or "Invalid target value type" # Optionally add to error

        log.debug(f"Check result: breached={is_breached}, error='{error_msg}'")
        return CheckDetails(
            is_breached=is_breached,
            actual_value=actual,
            target_value=target,
            lower_limit=lower,
            upper_limit=upper,
            error_message=error_msg,
        )


# Add other Checkers like StringMatcher, SetMembershipChecker etc.
