# guideline_interfaces.py (or similar)
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

# Import types from the previous file
from .types import ActionSuggestion, CheckDetails, date


class IRequiresDataKeys(ABC):
    """Interface for components that declare data key dependencies."""

    @abstractmethod
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        """Returns a list of data keys required by this component."""
        pass


class IValueCalculator(IRequiresDataKeys):
    """Strategy for calculating the value to be checked."""

    @abstractmethod
    async def calculate(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[Optional[Any], Optional[str]]:
        """
        Calculates the value from resolved data and parameters.
        Returns: (calculated_value, error_message_or_none)
        Marked async as it might involve async I/O.
        """
        pass


class IChecker(ABC):
    """Strategy for comparing a calculated value against guideline parameters."""

    @abstractmethod
    def check(
        self, value_to_check: Optional[Any], params: Dict[str, Any]
    ) -> CheckDetails:
        """
        Performs the check and returns CheckDetails.
        Typically CPU-bound, so usually not async.
        params contains limits like lower_limit, upper_limit.
        """
        pass


class ISuggestionGenerator(IRequiresDataKeys):
    """Strategy for generating suggestions based on check results."""

    @abstractmethod
    async def generate(
        self,
        details: CheckDetails,
        resolved_data: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Optional[ActionSuggestion]:
        """
        Generates an ActionSuggestion from CheckDetails, data, and parameters.
        Marked async if suggestion logic requires data access or is complex.
        """
        pass

    # get_required_data_keys might need to include keys for suggestion logic


# Optional: Combined interface if calculation, check, and suggestion are tightly coupled
class IGuidelineLogic(IRequiresDataKeys):
    """Combined interface for strategies performing calc, check, and suggestion."""

    @abstractmethod
    async def process(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[Optional[CheckDetails], Optional[ActionSuggestion], Optional[str]]:
        """
        Processes data to perform calc, check, suggestion.
        Returns: (CheckDetails or None, ActionSuggestion or None, Error Message or None)
        Facilitates building the GuidelineResult.
        """
        pass

    # get_required_data_keys is inherited from IRequiresDataKeys


# === Schedule Generator Interface ===


class IScheduleGenerator(IRequiresDataKeys):
    """Strategy for generating a list of schedule dates."""

    @abstractmethod
    async def generate(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[List[date], Optional[str]]:
        """
        Generates the schedule based on resolved data and parameters.
        Returns: (list_of_dates, error_message_or_none)
        Might be async due to date library calls or data access.
        """
        pass

    # get_required_data_keys is inherited from IRequiresDataKeys
