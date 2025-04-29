# calculator_components.py
import logging
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Sequence, Tuple  # Added Tuple

from .interfaces import IRequiresDataKeys  # Import from interfaces file
from .types import Position

log = logging.getLogger(__name__)


class IPositionFilter(IRequiresDataKeys):
    """Filters a list of positions based on specified criteria."""

    @abstractmethod
    def filter(
        self,
        positions: Sequence[Position],
        resolved_data: Dict[str, Any],
        params: Dict[str, Any],
    ) -> List[Position]:
        """Performs filtering and returns a list of matching positions."""
        pass


class IDenominatorProvider(IRequiresDataKeys):
    """Provides the denominator value for ratio calculations."""

    @abstractmethod
    async def get_denominator(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[Optional[float], Optional[str]]:
        """Calculates and returns the denominator value. (value, error_message_or_none)"""
        pass


# --- Concrete Implementations ---


class AssetClassFilter(IPositionFilter):
    """Filters positions by a specified asset class."""

    def __init__(self, class_key_suffix: str = ":class"):
        self.class_key_suffix = class_key_suffix

    def filter(
        self,
        positions: Sequence[Position],
        resolved_data: Dict[str, Any],
        params: Dict[str, Any],
    ) -> List[Position]:
        target_class = params.get("asset_class")
        if not target_class:
            log.warning(
                "AssetClassFilter: 'asset_class' not specified in filter params."
            )
            return []  # Or raise error, or return all positions?

        filtered = []
        for pos in positions:
            class_data_key = f"{pos.ticker}{self.class_key_suffix}"
            asset_class = resolved_data.get(class_data_key)
            if asset_class == target_class:
                filtered.append(pos)
            elif asset_class is None:
                log.debug(
                    f"Asset class data not found for {pos.ticker} (key: {class_data_key})"
                )
        log.debug(
            f"AssetClassFilter: Found {len(filtered)} positions for class '{target_class}'"
        )
        return filtered

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # Needs keys for asset class lookup for all potential positions. Dynamic key issue.
        log.warning("AssetClassFilter requires dynamic keys based on positions.")
        # Placeholder: Might need a way to know potential tickers upfront
        return []  # Placeholder implementation


class TotalPortfolioValueDenominator(IDenominatorProvider):
    """Provides the total market value of the portfolio as the denominator."""

    def __init__(
        self,
        holdings_key: str = "holdings",  # Key holding the list of position dicts/objects
        qty_suffix: str = ":qty",
        price_suffix: str = ":price",
    ):
        self.holdings_key = holdings_key
        self.qty_suffix = qty_suffix
        self.price_suffix = price_suffix

    async def get_denominator(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[Optional[float], Optional[str]]:
        positions_data = resolved_data.get(self.holdings_key)
        # Assuming positions_data is a list of dicts {'ticker': 'AAPL', ...}
        if not isinstance(positions_data, list):
            return (
                None,
                f"Denominator Error: '{self.holdings_key}' not found or not a list.",
            )

        total_value = 0.0
        calculation_errors = []

        # This part is unlikely to be async unless fetching prices inside, but keep async signature
        for pos_dict in positions_data:
            ticker = pos_dict.get("ticker")
            if not ticker:
                continue

            qty_key = f"{ticker}{self.qty_suffix}"
            price_key = f"{ticker}{self.price_suffix}"

            qty = resolved_data.get(qty_key)
            price = resolved_data.get(price_key)

            if qty is None or price is None:
                calculation_errors.append(f"Missing qty/price for {ticker}")
                continue  # Or treat as error and stop
            try:
                total_value += float(qty) * float(price)
            except (ValueError, TypeError) as e:
                calculation_errors.append(f"Invalid numeric data for {ticker}: {e}")

        if calculation_errors:
            log.warning(
                f"Denominator calculation encountered errors: {'; '.join(calculation_errors)}"
            )
            # Decide: return partial value or None? Returning None for failure.
            return None, "; ".join(calculation_errors)

        log.debug(f"Total portfolio value calculated: {total_value}")
        return total_value, None

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # Needs qty and price for all positions. Dynamic key issue.
        log.warning(
            "TotalPortfolioValueDenominator requires dynamic keys based on positions."
        )
        # The key for the holdings list itself is needed
        return [
            self.holdings_key
        ]  # Placeholder implementation (actual keys are many more)
