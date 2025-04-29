# guideline.py
import logging
import uuid
from typing import Any, Dict, List, Optional

from .interfaces import IChecker, ISuggestionGenerator, IValueCalculator

# Import necessary types and interfaces
from .types import (
    ActionSuggestion,
    CheckDetails,
    GuidelineCategory,
    GuidelineResult,
)

log = logging.getLogger(__name__)


class Guideline:
    """
    Represents a single guideline rule, composed of strategies for
    calculation, checking, and suggestion generation.
    """

    def __init__(
        self,
        category: GuidelineCategory,
        name: str,  # Human-readable name
        calculator: IValueCalculator,
        checker: IChecker,
        suggester: ISuggestionGenerator,
        params: Dict,  # Combined parameters for calc, check, suggest
        id: Optional[uuid.UUID] = None,
    ):
        self.id: uuid.UUID = id or uuid.uuid4()
        self.category = category
        self.name = name
        # --- Store Strategies ---
        if not isinstance(calculator, IValueCalculator):
            raise TypeError("calculator must implement IValueCalculator")
        if not isinstance(checker, IChecker):
            raise TypeError("checker must implement IChecker")
        if not isinstance(suggester, ISuggestionGenerator):
            raise TypeError("suggester must implement ISuggestionGenerator")
        self.calculator = calculator
        self.checker = checker
        self.suggester = suggester
        # --- Store Parameters ---
        self.params = params
        self._dependencies: Optional[List[str]] = None  # Cache for dependencies

        log.debug(f"Guideline '{self.name}' (ID: {self.id}) created.")

    def get_dependencies(self) -> List[str]:
        """Returns all data keys required by this guideline's components."""
        if self._dependencies is None:
            keys = set()
            try:
                keys.update(self.calculator.get_required_data_keys(self.params))
            except Exception as e:
                log.error(
                    f"Error getting keys from calculator for {self.name} ({self.id}): {e}"
                )
            # Checkers typically don't require data keys directly
            try:
                keys.update(self.suggester.get_required_data_keys(self.params))
            except Exception as e:
                log.error(
                    f"Error getting keys from suggester for {self.name} ({self.id}): {e}"
                )
            self._dependencies = list(keys)
            log.debug(f"Dependencies calculated for {self.name}: {self._dependencies}")
        return self._dependencies

    async def evaluate(self, resolved_data: Dict[str, Any]) -> GuidelineResult:
        """Evaluates the guideline using resolved data and returns the result."""
        log.info(f"Evaluating guideline: '{self.name}' (ID: {self.id})")
        actual_value: Optional[Any] = None
        calc_error: Optional[str] = None
        check_details: Optional[CheckDetails] = None
        suggestion: Optional[ActionSuggestion] = None
        process_error: Optional[str] = None  # Overall process error message

        try:
            # --- 1. Calculate Value ---
            log.debug("Calculating value...")
            actual_value, calc_error = await self.calculator.calculate(
                resolved_data, self.params
            )
            if calc_error:
                log.warning(f"Calculation error for '{self.name}': {calc_error}")
                # Proceed to check, Checker should handle None or error cases

            # --- 2. Perform Check ---
            log.debug(f"Performing check for value: {actual_value}")
            # Pass the combined params dict to the checker
            check_details = self.checker.check(actual_value, self.params)
            log.debug(
                f"Check details: breached={check_details.is_breached}, error={check_details.error_message}"
            )

            # Prioritize check error message, then calculation error
            if check_details.error_message:
                process_error = f"Check Error: {check_details.error_message}"
                log.warning(
                    f"Check error for '{self.name}': {check_details.error_message}"
                )
            elif calc_error:
                process_error = f"Calculation Error: {calc_error}"
                # Breach status might depend on whether calc error implies breach
                if check_details is None:  # Should not happen if checker handles None
                    check_details = CheckDetails(
                        is_breached=True,
                        actual_value=None,
                        target_value=self.params.get("target"),
                        lower_limit=self.params.get("lower_limit"),
                        upper_limit=self.params.get("upper_limit"),
                        error_message=process_error,
                    )
                else:
                    # Mark as breached if calculation failed? Or let checker decide?
                    # Let's assume checker handles None value appropriately.
                    # If check didn't explicitly breach, but calc failed, maybe force breach?
                    # check_details.is_breached = check_details.is_breached or True # Optional: Force breach on calc error
                    pass

            # --- 3. Generate Suggestion ---
            if check_details is not None:  # Only generate if check details exist
                log.debug("Generating suggestion...")
                suggestion = await self.suggester.generate(
                    check_details, resolved_data, self.params
                )
                log.debug(f"Suggestion generated: {suggestion}")
            else:
                # Should not happen if checker always returns CheckDetails
                log.error(
                    f"Cannot generate suggestion for '{self.name}' as CheckDetails are missing."
                )
                process_error = (
                    process_error or "Internal Error: CheckDetails missing after check."
                )
                suggestion = ActionSuggestion(
                    "内部エラーのため提案できません", severity="Error"
                )

        except Exception as e:
            log.exception(
                f"Unexpected error during guideline evaluation for {self.name} ({self.id}): {e}",
                exc_info=True,
            )
            process_error = f"Unexpected evaluation error: {e}"
            # Create default error objects if needed
            if check_details is None:
                check_details = CheckDetails(
                    is_breached=True,
                    actual_value=actual_value,
                    target_value=self.params.get("target"),
                    lower_limit=self.params.get("lower_limit"),
                    upper_limit=self.params.get("upper_limit"),
                    error_message=process_error,
                )
            if suggestion is None:
                suggestion = ActionSuggestion(
                    "Evaluation error occurred.",
                    severity="Error",
                    details={"exception": str(e)},
                )

        # --- 4. Assemble GuidelineResult ---
        if check_details is None:  # Should be impossible now, but defensive check
            log.critical(
                f"CRITICAL: CheckDetails is None for '{self.name}'. Creating minimal error result."
            )
            return GuidelineResult(
                guideline_id=self.id,
                guideline_name=self.name,
                guideline_category=self.category,
                is_breached=True,
                actual_value=None,
                target_value=None,
                lower_limit=None,
                upper_limit=None,
                suggestion=ActionSuggestion(
                    "Critical internal error", severity="Critical"
                ),
                error_message=process_error or "CheckDetails generation failed",
            )

        final_result = GuidelineResult(
            guideline_id=self.id,
            guideline_name=self.name,
            guideline_category=self.category,
            is_breached=check_details.is_breached
            or bool(process_error),  # Treat any process error as a breach indicator
            actual_value=check_details.actual_value,
            target_value=check_details.target_value,
            lower_limit=check_details.lower_limit,
            upper_limit=check_details.upper_limit,
            suggestion=suggestion,
            raw_check_value=actual_value,  # Store the value passed to the checker
            error_message=process_error,
        )
        log.info(
            f"Evaluation complete for '{self.name}'. Result Status: {'BREACHED' if final_result.is_breached else 'OK'}{' (Error: ' + process_error + ')' if process_error else ''}"
        )
        return final_result

    def __repr__(self):
        # Provide a concise representation
        return f"<Guideline id={str(self.id)[:8]} name='{self.name}' category={self.category.name}>"

    def __hash__(self):
        # Hash based on the unique ID
        return hash(self.id)

    def __eq__(self, other):
        # Equality based on the unique ID
        if isinstance(other, Guideline):
            return self.id == other.id
        return NotImplemented
