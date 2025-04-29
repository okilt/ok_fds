# suggesters.py
import logging
from typing import Any, Dict, List, Optional

from .interfaces import ISuggestionGenerator
from .types import ActionSuggestion, CheckDetails

log = logging.getLogger(__name__)


class BasicRangeSuggester(ISuggestionGenerator):
    """Generates simple informational messages based on range check results."""

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # This simple suggester doesn't require additional data keys beyond check details
        return []

    async def generate(
        self,
        details: CheckDetails,
        resolved_data: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Optional[ActionSuggestion]:
        log.debug(
            f"Generating suggestion for check details: breached={details.is_breached}, actual={details.actual_value}"
        )

        if details.error_message:
            # If the check itself had an error, reflect that
            return ActionSuggestion(
                f"Cannot suggest due to check error: {details.error_message}",
                severity="Error",
            )

        if details.actual_value is None:
            return ActionSuggestion(
                "No value to check, cannot generate suggestion.", severity="Error"
            )

        if not details.is_breached:
            # Generate informational message for non-breached cases
            msg = "Value is within limits."
            severity = "Info"
            # Compare with target if available
            if details.target_value is not None and details.actual_value is not None:
                try:
                    target_f = float(details.target_value)
                    actual_f = float(details.actual_value)
                    diff = actual_f - target_f
                    tolerance = 1e-9
                    if abs(diff) < tolerance:
                        msg += " Matches target value."
                    elif diff < 0:
                        msg += f" Below target ({target_f:.4f}) by {abs(diff):.4f}."
                    else:  # diff > 0
                        msg += f" Above target ({target_f:.4f}) by {diff:.4f}."
                except (ValueError, TypeError):
                    msg += f" Target ({details.target_value}) comparison not applicable."  # Handle non-numeric target/actual
            return ActionSuggestion(msg, severity=severity)
        else:
            # Generate message for breached cases
            severity = "Warning"  # Default severity for breach
            msg = f"Value is outside limits (Actual: {details.actual_value:.4f})"
            required_change = None
            action_type = "adjust"  # Generic action type

            try:
                actual_f = float(details.actual_value)
                # Check against lower limit
                if details.lower_limit is not None:
                    lower_f = float(details.lower_limit)
                    if actual_f < lower_f:
                        msg += f". Below lower limit ({lower_f:.4f})."
                        required_change = lower_f - actual_f  # Positive change needed
                        action_type = "increase"

                # Check against upper limit (only if lower wasn't breached)
                if (
                    details.upper_limit is not None and action_type == "adjust"
                ):  # Check if lower limit breach already identified
                    upper_f = float(details.upper_limit)
                    if actual_f > upper_f:
                        msg += f". Above upper limit ({upper_f:.4f})."
                        required_change = (
                            upper_f - actual_f
                        )  # Negative change needed (difference *to* limit)
                        action_type = "decrease"

                if required_change is not None:
                    verb = "Increase" if action_type == "increase" else "Decrease"
                    msg += f" Requires {verb} of at least {abs(required_change):.4f}."

            except (ValueError, TypeError) as e:
                msg += f" Error comparing limits ({e})"
                severity = "Error"  # Upgrade severity if limits are invalid

            # Placeholder for more complex suggestion details (e.g., trade quantity)
            # These could be calculated here using resolved_data or by another dedicated suggester component.
            suggestion_details = {
                "required_change": required_change,
                "action_type": action_type,
            }

            return ActionSuggestion(msg, severity=severity, details=suggestion_details)


# Add other Suggesters like TradeQuantitySuggester, etc.
