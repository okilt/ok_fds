# value_calculators.py
import logging
from typing import Any, Dict, List, Optional, Tuple  # Added Sequence

from .calculator_components import (
    AssetClassFilter,
    IDenominatorProvider,
    IPositionFilter,
)
from .interfaces import IValueCalculator
from .types import Position  # Import Position type

log = logging.getLogger(__name__)


class AllocationCalculator(IValueCalculator):
    """
    Calculates allocation ratio using a specified filter and denominator provider.
    """

    def __init__(
        self,
        position_filter: IPositionFilter,
        denominator_provider: IDenominatorProvider,
        holdings_key: str = "holdings",  # Key for raw position data (e.g., list of dicts)
        # position_list_key: Optional[str] = None, # Optional key if Resolver provides Position objects
        qty_suffix: str = ":qty",
        price_suffix: str = ":price",
    ):
        self.filter = position_filter
        self.denominator_provider = denominator_provider
        self.holdings_key = holdings_key
        # self.position_list_key = position_list_key or holdings_key
        self.qty_suffix = qty_suffix
        self.price_suffix = price_suffix

    async def calculate(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[Optional[float], Optional[str]]:
        log.debug(
            f"Calculating allocation with filter params: {params.get('filter_params')}"
        )

        # --- 1. Get Position Data ---
        raw_positions = resolved_data.get(self.holdings_key)
        if not isinstance(raw_positions, list):
            return (
                None,
                f"Calculation Error: '{self.holdings_key}' not found or not a list.",
            )

        # --- Convert raw data to Position objects (or assume Resolver does this) ---
        # This example assumes we get raw dicts and build simple Position objects
        all_positions = []
        position_keys_needed = set()  # Track keys needed for valuation later
        for pos_data in raw_positions:
            ticker = pos_data.get("ticker")
            qty_key = f"{ticker}{self.qty_suffix}"
            price_key = f"{ticker}{self.price_suffix}"
            qty_val = resolved_data.get(
                qty_key
            )  # Quantity needed to build Position obj
            if ticker and qty_val is not None:
                try:
                    # Store ticker and quantity; other data (price, class) looked up later
                    all_positions.append(
                        Position(ticker=ticker, quantity=float(qty_val))
                    )
                    position_keys_needed.add(price_key)  # Need price later
                    # Add class key if filter needs it (AssetClassFilter does)
                    if isinstance(self.filter, AssetClassFilter):
                        position_keys_needed.add(
                            f"{ticker}{self.filter.class_key_suffix}"
                        )
                except ValueError:
                    log.warning(f"Invalid quantity for {ticker}, skipping position.")
            elif ticker:
                log.warning(f"Missing quantity for {ticker}, skipping position.")
        # Note: get_required_data_keys should ideally return position_keys_needed too

        # --- 2. Filter Positions ---
        try:
            # Pass resolved_data to filter in case it needs other info (like asset class)
            filtered_positions = self.filter.filter(
                all_positions, resolved_data, params.get("filter_params", {})
            )
        except Exception as e:
            log.error(f"Error during position filtering: {e}", exc_info=True)
            return None, f"Filtering Error: {e}"

        # --- 3. Calculate Numerator (Sum of filtered positions' market values) ---
        numerator_value = 0.0
        calculation_errors = []
        for pos in filtered_positions:
            price_key = f"{pos.ticker}{self.price_suffix}"
            price = resolved_data.get(price_key)
            if price is None:
                calculation_errors.append(f"Missing price for {pos.ticker}")
                continue
            try:
                # Use quantity stored in Position object
                numerator_value += pos.quantity * float(price)
            except (ValueError, TypeError) as e:
                calculation_errors.append(f"Invalid numeric data for {pos.ticker}: {e}")

        if calculation_errors:
            log.warning(
                f"Numerator calculation encountered errors: {'; '.join(calculation_errors)}"
            )
            # Decide: Fail calculation or proceed with partial value? Failing here.
            # return None, f"Numerator Calculation Error: {'; '.join(calculation_errors)}"

        # --- 4. Get Denominator ---
        denominator_value, den_err = await self.denominator_provider.get_denominator(
            resolved_data, params.get("denominator_params", {})
        )
        if den_err:
            return None, f"Denominator Error: {den_err}"
        if denominator_value is None:
            return None, "Denominator could not be calculated."

        # --- 5. Calculate Ratio ---
        if abs(denominator_value) < 1e-9:  # Check for zero division
            if abs(numerator_value) < 1e-9:
                return 0.0, None  # Treat 0 / 0 as 0
            else:
                return None, "Denominator is zero"
        if not isinstance(numerator_value, (int, float)) or not isinstance(
            denominator_value, (int, float)
        ):
            return None, "Numerator or Denominator is not a valid number for division."

        ratio = numerator_value / denominator_value
        log.debug(
            f"Calculated ratio: {ratio:.4f} (Num: {numerator_value:.2f}, Den: {denominator_value:.2f})"
        )
        return ratio, None  # Success

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # Combine keys from filter, denominator, and potentially valuation needs.
        keys = set()
        keys.add(self.holdings_key)  # Key for the list of positions/holdings
        keys.update(self.filter.get_required_data_keys(params.get("filter_params", {})))
        keys.update(
            self.denominator_provider.get_required_data_keys(
                params.get("denominator_params", {})
            )
        )

        # The big challenge: Needs keys for qty, price (and maybe class) for *all* potential positions.
        # This often requires prior knowledge of the portfolio universe or dynamic fetching.
        log.warning(
            "AllocationCalculator requires dynamic keys for quantities and prices based on holdings. Returning only static keys."
        )
        # For a static approach, you might need a param listing expected tickers.
        # Example: expected_tickers = params.get('universe_tickers', [])
        # for ticker in expected_tickers:
        #     keys.add(f"{ticker}{self.qty_suffix}")
        #     keys.add(f"{ticker}{self.price_suffix}")
        #     # Add class keys if needed by filter
        #     if isinstance(self.filter, AssetClassFilter):
        #          keys.add(f"{ticker}{self.filter.class_key_suffix}")

        return list(keys)  # Placeholder implementation


class DirectValueExtractor(IValueCalculator):
    """Simple calculator that extracts a value directly from a key."""

    def __init__(self, key_to_extract: str):
        self.key = key_to_extract
        log.debug(f"DirectValueExtractor initialized for key: {self.key}")

    async def calculate(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[Optional[Any], Optional[str]]:
        log.debug(f"Extracting value for key: {self.key}")
        val = resolved_data.get(self.key)
        if val is None:
            log.warning(f"Key '{self.key}' not found in resolved data.")
            return None, f"Key '{self.key}' not found in resolved data."
        log.debug(f"Value extracted: {val}")
        return val, None

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        return [self.key]
