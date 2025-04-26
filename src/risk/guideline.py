import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Callable, Tuple

# --- Data Structures ---

@dataclass
class ActionSuggestion:
    """Represents a suggested action based on a guideline check."""
    message: str
    severity: str = "Info"  # e.g., Info, Warning, Error
    details: Optional[Dict[str, Any]] = None # e.g., {'quantity': 10, 'type': 'buy', 'ticker': 'AAPL'}

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
    error_message: Optional[str] = None # For check-related errors

@dataclass
class GuidelineResult:
    """Final result object for a guideline check, including suggestion."""
    guideline_id: str
    is_breached: bool
    actual_value: Optional[Any]
    target_value: Optional[Any]
    lower_limit: Optional[Any]
    upper_limit: Optional[Any]
    check_data: Dict[str, Any] # Original raw data used
    suggestion: Optional[ActionSuggestion]
    error_message: Optional[str] = None # For any errors during the process

    def __repr__(self):
        status = "BREACHED" if self.is_breached else "OK"
        if self.error_message:
            status = f"ERROR ({self.error_message})"
        return (f"Result(guideline='{self.guideline_id}', status={status}, "
                f"actual={self.actual_value:.4f}, target={self.target_value}, " # Added formatting for clarity
                f"suggestion={self.suggestion})")

# --- Core Interfaces / Abstract Base Classes ---

class INumeratorCalculator(ABC):
    """Calculates the numerator value for a ratio."""
    @abstractmethod
    def calculate(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        """
        Calculates the numerator.
        Returns a tuple: (calculated_value, error_message).
        params might contain specifics like 'ticker' or 'asset_class'.
        """
        pass

    @abstractmethod
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        """Returns data keys needed by this calculator based on params."""
        pass

    @abstractmethod
    def get_price_key_for_suggestion(self, params: Dict[str, Any]) -> Optional[str]:
        """Returns the relevant price key from raw_data needed for suggestion quantity calculation."""
        pass


class IDenominatorCalculator(ABC):
    """Calculates the denominator value for a ratio."""
    @abstractmethod
    def calculate(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        """
        Calculates the denominator.
        Returns a tuple: (calculated_value, error_message).
        """
        pass

    @abstractmethod
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        """Returns data keys needed by this calculator based on params."""
        pass


class IValueSuggester(ABC):
    """
    Calculates the value to be checked AND the suggestion based on check results.
    Encapsulates the relationship between value calculation and suggestion logic.
    """
    @abstractmethod
    def calculate_value(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[Any], Optional[str]]:
        """
        Calculates the primary value to be checked by the IChecker.
        Returns a tuple: (calculated_value, error_message).
        """
        pass

    @abstractmethod
    def calculate_suggestion(self, details: CheckDetails, raw_data: Dict[str, Any], params: Dict[str, Any]) -> ActionSuggestion:
        """
        Calculates the action suggestion based on check details and raw data.
        This is where the inverse calculation logic often resides.
        """
        pass

    @abstractmethod
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        """Returns all data keys needed by this value suggester (incl. underlying calculators)."""
        pass


class IChecker(ABC):
    """Performs a pure check on a calculated value against guideline parameters."""
    @abstractmethod
    def check(self, guideline_params: Dict[str, Any], value_to_check: Optional[Any]) -> CheckDetails:
        """
        Checks the value against limits/target defined in guideline_params.
        Returns CheckDetails.
        """
        pass
    

# --- Concrete Calculator Implementations ---

def _get_value(data: Dict, key: str, expected_type: type = float) -> Tuple[Optional[Any], Optional[str]]:
    """Helper to safely get and type-check data."""
    if key not in data:
        return None, f"Missing data key: {key}"
    val = data[key]
    if val is None:
        return None, None # Allow None values if present
    try:
        # Special case for bool stored as 0/1 or "true"/"false" if needed
        if expected_type == bool:
             if isinstance(val, str):
                  if val.lower() == 'true': return True, None
                  if val.lower() == 'false': return False, None
             # Fallback to standard bool conversion
             return bool(expected_type(val)), None

        return expected_type(val), None
    except (ValueError, TypeError) as e:
        return None, f"Invalid type for key {key}. Expected {expected_type.__name__}, got {type(val).__name__}. Error: {e}"

class TotalPortfolioValueCalculator(INumeratorCalculator, IDenominatorCalculator):
    """Calculates the total market value of the portfolio based on listed holdings."""

    def __init__(self, holdings_key: str = 'holdings', ticker_field: str = 'ticker', qty_suffix: str = ':qty', price_suffix: str = ':price'):
        self.holdings_key = holdings_key
        self.ticker_field = ticker_field
        self.qty_suffix = qty_suffix
        self.price_suffix = price_suffix

    def calculate(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        holdings = raw_data.get(self.holdings_key)
        if not isinstance(holdings, list):
            return None, f"'{self.holdings_key}' key not found or not a list in raw_data"

        total_value = 0.0
        calculation_errors = []

        for holding in holdings:
            if not isinstance(holding, dict):
                 calculation_errors.append(f"Item in '{self.holdings_key}' is not a dictionary: {holding}")
                 continue # Skip this item

            ticker = holding.get(self.ticker_field)
            if not ticker:
                 calculation_errors.append(f"Missing '{self.ticker_field}' in holding: {holding}")
                 continue # Skip holding without a ticker

            qty_key = f"{ticker}{self.qty_suffix}"
            price_key = f"{ticker}{self.price_suffix}"

            qty, qty_err = _get_value(raw_data, qty_key, float)
            price, price_err = _get_value(raw_data, price_key, float)

            if qty_err: calculation_errors.append(qty_err); continue
            if price_err: calculation_errors.append(price_err); continue
            if qty is None or price is None:
                 calculation_errors.append(f"Missing quantity or price for {ticker}")
                 continue # Skip calculation for this holding

            total_value += qty * price

        if calculation_errors:
            # Return 0 or None depending on how errors should be treated
            # Returning None might be safer if partial calculation is misleading
            error_summary = "; ".join(calculation_errors)
            print(f"Warning during TotalPortfolioValue calculation: {error_summary}")
            # Decide if calculation should proceed with partial data or fail
            # For now, let's return the calculated value but signal errors
            # return total_value, error_summary # Option 1: Return value + error message
            return None, error_summary # Option 2: Fail calculation if any error occurs

        return total_value, None

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # This ideally needs access to the actual holdings list to be accurate.
        # For a static implementation, assume a predefined universe or require keys upfront.
        # Placeholder - Requires improvement in a real system.
        # Option: Require 'holdings_list' in params?
        print(f"WARN: Dynamic keys required for {self.__class__.__name__}. Returning generic keys based on common pattern.")
        # Example: Return the holdings key itself. Specific item keys are derived dynamically.
        return [self.holdings_key] # Caller needs to ensure individual keys exist based on holdings

    def get_price_key_for_suggestion(self, params: Dict[str, Any]) -> Optional[str]:
        # Not applicable when calculating total value itself
        return None


class SingleStockValueCalculator(INumeratorCalculator):
    """Calculates the market value of a single stock."""
    def __init__(self, qty_suffix: str = ':qty', price_suffix: str = ':price'):
        self.qty_suffix = qty_suffix
        self.price_suffix = price_suffix

    def calculate(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        ticker = params.get('target_identifier')
        if not ticker or not isinstance(ticker, str):
            return None, "Missing or invalid 'target_identifier' (ticker) in params"

        qty_key = f"{ticker}{self.qty_suffix}"
        price_key = f"{ticker}{self.price_suffix}"

        qty, qty_err = _get_value(raw_data, qty_key, float)
        price, price_err = _get_value(raw_data, price_key, float)

        if qty_err: return None, qty_err
        if price_err: return None, price_err
        if qty is None or price is None:
             # Consider if 0 quantity is valid or should be treated as missing data
             # If qty is 0, value is 0, which is valid. If price is missing, it's an error.
             if price is None: return None, f"Missing price for {ticker}"
             if qty is None: return None, f"Missing quantity for {ticker}" # Or return 0?

        # Ensure qty and price are numbers before multiplying
        if not isinstance(qty, (int, float)) or not isinstance(price, (int, float)):
             return None, f"Quantity or Price for {ticker} is not a number after retrieval."

        return qty * price, None

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        ticker = params.get('target_identifier')
        if not ticker or not isinstance(ticker, str): return []
        return [f"{ticker}{self.qty_suffix}", f"{ticker}{self.price_suffix}"]

    def get_price_key_for_suggestion(self, params: Dict[str, Any]) -> Optional[str]:
        ticker = params.get('target_identifier')
        if not ticker or not isinstance(ticker, str): return None
        return f"{ticker}{self.price_suffix}"


class AssetClassValueCalculator(INumeratorCalculator):
    """Calculates the total market value of assets belonging to a specific class."""
    def __init__(self, holdings_key: str = 'holdings', ticker_field: str = 'ticker', class_key_suffix: str = ':class',
                 qty_suffix: str = ':qty', price_suffix: str = ':price'):
        self.holdings_key = holdings_key
        self.ticker_field = ticker_field
        self.class_key_suffix = class_key_suffix
        self.qty_suffix = qty_suffix
        self.price_suffix = price_suffix

    def calculate(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        target_class = params.get('target_identifier')
        if not target_class or not isinstance(target_class, str):
            return None, "Missing or invalid 'target_identifier' (asset_class) in params"

        holdings = raw_data.get(self.holdings_key)
        if not isinstance(holdings, list):
             return None, f"'{self.holdings_key}' key not found or not a list in raw_data"

        class_value = 0.0
        processed_tickers = set()
        calculation_errors = []

        for holding in holdings:
            if not isinstance(holding, dict):
                 calculation_errors.append(f"Item in '{self.holdings_key}' is not a dictionary: {holding}")
                 continue

            ticker = holding.get(self.ticker_field)
            if not ticker or not isinstance(ticker, str) or ticker in processed_tickers:
                 if not ticker: calculation_errors.append(f"Missing '{self.ticker_field}' in holding: {holding}")
                 continue
            processed_tickers.add(ticker)

            # Check if the ticker belongs to the target asset class
            ticker_class_key = f"{ticker}{self.class_key_suffix}"
            ticker_class, class_err = _get_value(raw_data, ticker_class_key, str)

            if class_err:
                calculation_errors.append(f"Cannot determine class for {ticker}: {class_err}")
                continue # Skip ticker if class info is missing/invalid
            if ticker_class != target_class:
                continue # Skip ticker if not in the target class

            # Calculate value for this ticker if it's in the target class
            qty_key = f"{ticker}{self.qty_suffix}"
            price_key = f"{ticker}{self.price_suffix}"
            qty, qty_err = _get_value(raw_data, qty_key, float)
            price, price_err = _get_value(raw_data, price_key, float)

            if qty_err: calculation_errors.append(f"{ticker}: {qty_err}"); continue
            if price_err: calculation_errors.append(f"{ticker}: {price_err}"); continue
            if qty is None or price is None:
                 calculation_errors.append(f"Missing quantity or price for {ticker} in target class {target_class}")
                 continue

            if not isinstance(qty, (int, float)) or not isinstance(price, (int, float)):
                 calculation_errors.append(f"Quantity or Price for {ticker} is not a number.")
                 continue

            class_value += qty * price

        if calculation_errors:
            error_summary = "; ".join(calculation_errors)
            print(f"Warning during AssetClassValue calculation for '{target_class}': {error_summary}")
            # Decide: return partial value or fail? Let's return partial for now.
            # return None, error_summary # Option: Fail if errors
            return class_value, error_summary # Option: Return partial value + errors

        return class_value, None

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # Still dynamic. Needs improvement.
        print(f"WARN: Dynamic keys required for {self.__class__.__name__}. Returning generic key.")
        return [self.holdings_key] # Caller must ensure underlying keys exist

    def get_price_key_for_suggestion(self, params: Dict[str, Any]) -> Optional[str]:
        # For an asset class, there isn't one single price for inverse calculation.
        # Suggestion might be value-based or use a representative ETF price.
        return params.get('representative_price_key') # Allow override via params 
    
class RatioValueSuggester(IValueSuggester):
    """
    Calculates a ratio and suggests actions based on it.
    Uses injected calculators for numerator and denominator.
    Assumes suggestion involves inverse calculation using denominator and price.
    """
    def __init__(self,
                 numerator_calculator: INumeratorCalculator,
                 denominator_calculator: IDenominatorCalculator,
                 rounding_precision: int = 4): # Precision for suggestion quantity
        self.numerator_calc = numerator_calculator
        self.denominator_calc = denominator_calculator
        self.rounding_precision = rounding_precision
        self._last_denominator: Optional[float] = None # Store for suggestion calc

    def calculate_value(self, raw_data: Dict[str, Any], params: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        self._last_denominator = None # Reset before calculation

        num, num_err = self.numerator_calc.calculate(raw_data, params)
        if num_err: return None, f"Numerator Error: {num_err}"
        # Allow num to be 0, but None is usually an error or indicates missing data
        if num is None: return None, "Numerator calculated as None (check input data or calc logic)"

        den, den_err = self.denominator_calc.calculate(raw_data, params)
        if den_err: return None, f"Denominator Error: {den_err}"
        if den is None: return None, "Denominator calculated as None"
        if abs(den) < 1e-9: # Check for near-zero denominator
            # Return 0 if numerator is also 0, otherwise it's an error/undefined
            if abs(num) < 1e-9:
                 self._last_denominator = 0.0
                 return 0.0, None
            else:
                 return None, "Denominator is zero or near-zero"

        self._last_denominator = den # Store for suggestion
        # Ensure both are numbers before division
        if not isinstance(num, (int, float)) or not isinstance(den, (int, float)):
             return None, f"Numerator or Denominator is not a number ({type(num)}, {type(den)})"

        return num / den, None

    def calculate_suggestion(self, details: CheckDetails, raw_data: Dict[str, Any], params: Dict[str, Any]) -> ActionSuggestion:
        # Handle case where check itself failed
        if details.error_message:
            # If the value couldn't be checked, generate an error suggestion
            return ActionSuggestion(f"Suggestion not possible due to check error: {details.error_message}", severity="Error")

        actual = details.actual_value
        target = details.target_value
        lower = details.lower_limit
        upper = details.upper_limit
        denominator = self._last_denominator # Use stored value from calculate_value

        # --- Default / OK state ---
        severity = "Info"
        message = f"Current: {actual:.2%}" if isinstance(actual, (int, float)) else f"Current: N/A ({actual})"
        action_type = "hold"
        required_change_pct = 0.0
        suggested_quantity = None
        trade_details = {}

        # Check if calculation of 'actual' failed earlier (indicated by None perhaps)
        if actual is None:
            # This case should ideally be covered by details.error_message, but double-check
             return ActionSuggestion(f"{message}. Cannot determine suggestion due to missing actual value.", severity="Error")

        # --- Determine Breach / Deviation ---
        if details.is_breached:
            severity = "Warning"
            if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
                message += ". Limits invalid." # Should be caught by Checker earlier
            elif actual < lower:
                required_change_pct = lower - actual
                action_type = "buy"
                message += f" (below lower limit of {lower:.2%})."
            elif actual > upper:
                required_change_pct = upper - actual # Difference to bring it back *to* the limit
                action_type = "sell"
                message += f" (above upper limit of {upper:.2%})."
            else:
                 # This case implies is_breached=True but value is within limits?
                 # Could happen if Checker had an internal error marked as breach.
                 message += ". Breach status inconsistent with limits."
                 severity = "Error"
        # Check deviation from target if not breached (and target exists)
        elif isinstance(target, (int, float)) and isinstance(actual, (int, float)):
            required_change_pct = target - actual
            tolerance = 1e-9 # Tolerance for float comparison
            if required_change_pct > tolerance:
                 action_type = "adjust_buy"
                 message += f" (below target of {target:.2%})."
            elif required_change_pct < -tolerance:
                 action_type = "adjust_sell"
                 message += f" (above target of {target:.2%})."
            else:
                 # Exactly on target
                 message += ". Matches target value."
                 action_type = "hold" # Redundant, but clear
        else:
            # Not breached, but target is missing or invalid - just report status
            message += ". Within limits."
            action_type = "hold"

        # --- Perform Inverse Calculation for Quantity (if possible) ---
        price_key = self.numerator_calc.get_price_key_for_suggestion(params)
        price, price_err = None, "Price key for suggestion not specified by numerator calculator"
        if price_key:
             price, price_err = _get_value(raw_data, price_key, float)
             if price_err: price = None # Nullify price if error getting it

        if action_type not in ["hold"] and denominator is not None and abs(denominator) > 1e-9 and price is not None and abs(price) > 1e-9:
            try:
                required_value_change = required_change_pct * denominator
                suggested_quantity_raw = required_value_change / price

                # Rounding logic (adjust precision as needed)
                # Use ceiling for buys (ensure limit is met), floor for sells (ensure limit is met)
                scale = 10**self.rounding_precision
                if suggested_quantity_raw > 0: # Buy or Adjust Buy
                    suggested_quantity = math.ceil(suggested_quantity_raw * scale) / scale
                elif suggested_quantity_raw < 0: # Sell or Adjust Sell
                    suggested_quantity = math.floor(suggested_quantity_raw * scale) / scale
                else:
                    suggested_quantity = 0.0

                # Avoid suggesting zero quantity if a non-zero change is needed (due to rounding)
                if abs(suggested_quantity) < 1e-9 and abs(required_change_pct) > 1e-9 :
                     # Suggest minimum possible unit if rounding resulted in zero
                      suggested_quantity = math.copysign(1 / scale if scale > 0 else 1, suggested_quantity_raw)

                if abs(suggested_quantity) > 1e-9:
                     trade_verb = action_type.replace('adjust_', '')
                     message += f" Suggested action: {trade_verb.capitalize()} approx. {abs(suggested_quantity):.{self.rounding_precision}f} units."
                     trade_details = {
                         'quantity': abs(suggested_quantity),
                         'type': trade_verb,
                         'ticker': params.get('target_identifier') if isinstance(self.numerator_calc, SingleStockValueCalculator) else None, # Add ticker if relevant
                         'value_change_est': suggested_quantity * price # Estimated value
                     }
                     trade_details = {k: v for k, v in trade_details.items() if v is not None} # Clean None values
                else:
                     message += " Required adjustment is below minimum precision."
                     action_type = "hold" # Revert action if quantity is zero


            except (TypeError, ValueError, OverflowError) as e:
                 message += f" Could not calculate quantity ({e})."
                 trade_details = {'error': str(e)}

        elif action_type not in ["hold"]:
             # Cannot calculate quantity (missing price/denom or they are zero)
             required_value_change = None
             if denominator is not None:
                 try:
                     required_value_change = required_change_pct * denominator
                     message += f" Suggested action: Adjust value by approx. {required_value_change:,.2f} ({action_type.replace('adjust_', '')} direction)."
                     trade_details = {'type': action_type.replace('adjust_', ''), 'value_change_est': required_value_change}
                 except (TypeError, ValueError, OverflowError) as e:
                      message += f" Adjustment needed in {action_type.replace('adjust_', '')} direction (value calculation error: {e})."
                      trade_details = {'type': action_type.replace('adjust_', ''), 'error': str(e)}

             else: # Denominator was None
                 message += f" Adjustment needed in {action_type.replace('adjust_', '')} direction (quantity/value calculation not possible - denominator missing)."
                 trade_details = {'type': action_type.replace('adjust_', ''), 'error': 'Denominator missing'}


        return ActionSuggestion(message, severity=severity, details=trade_details if trade_details else None)


    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # Combine keys from both calculators
        keys = set(self.numerator_calc.get_required_data_keys(params))
        keys.update(self.denominator_calc.get_required_data_keys(params))
        # Add keys needed directly by suggestion logic (e.g., price)
        price_key = self.numerator_calc.get_price_key_for_suggestion(params)
        if price_key:
            keys.add(price_key)
        return list(keys)
    
class FloatRangeChecker(IChecker):
    """Checks if a float value is within the specified range [lower_limit, upper_limit]."""
    def check(self, guideline_params: Dict[str, Any], value_to_check: Optional[Any]) -> CheckDetails:
        actual = value_to_check
        target = guideline_params.get('target') # Target is for info/suggestion, not check itself
        lower = guideline_params.get('lower_limit')
        upper = guideline_params.get('upper_limit')
        error_msg = None
        is_breached = False

        # --- Parameter Validation ---
        if lower is None or upper is None:
             error_msg = "Guideline configuration error: Missing lower_limit or upper_limit"
             is_breached = True # Treat config error as breach
        elif not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
             error_msg = f"Guideline configuration error: lower_limit ({type(lower).__name__}) or upper_limit ({type(upper).__name__}) is not a number"
             is_breached = True
        elif lower > upper:
             error_msg = f"Guideline configuration error: lower_limit ({lower}) is greater than upper_limit ({upper})"
             is_breached = True
        # Validate target type if present (but don't breach for invalid target type)
        if target is not None and not isinstance(target, (int, float)):
             # Log a warning, but don't fail the check itself for this
             print(f"Warning: Guideline target ({target}) is not a number ({type(target).__name__})")
             # error_msg = error_msg or "Guideline target is not a number" # Optionally add to error

        # --- Value Check ---
        if not is_breached: # Only check value if params are valid so far
            if actual is None:
                error_msg = "Value to check is None (cannot perform range check)"
                is_breached = True # Treat missing value as breach
            elif not isinstance(actual, (int, float)):
                 error_msg = f"Value to check is not a number (type: {type(actual).__name__})"
                 is_breached = True
            else:
                 # Perform the actual range check
                 if not (lower <= actual <= upper):
                      is_breached = True
                 # Check for NaN values which fail comparisons
                 if math.isnan(actual):
                      error_msg = "Value to check is NaN"
                      is_breached = True


        return CheckDetails(
            is_breached=is_breached,
            actual_value=actual,
            target_value=target,
            lower_limit=lower,
            upper_limit=upper,
            error_message=error_msg
        )
        
class Guideline:
    """
    Represents a single guideline rule and orchestrates its check.
    Uses injected IValueSuggester and IChecker components.
    """
    def __init__(self, id: str,
                 value_suggester: IValueSuggester,
                 checker: IChecker,
                 params: Dict[str, Any]):
        self.id = id
        if not isinstance(value_suggester, IValueSuggester):
            raise TypeError("value_suggester must implement IValueSuggester")
        if not isinstance(checker, IChecker):
            raise TypeError("checker must implement IChecker")

        self.value_suggester = value_suggester
        self.checker = checker
        # Params contains checker limits (lower, upper, target)
        # AND suggester/calculator params (target_identifier, etc.)
        self.params = params
        # Data keys are derived from the suggester, as it encapsulates calculation needs
        try:
             self._data_keys = self.value_suggester.get_required_data_keys(self.params)
        except Exception as e:
             print(f"Error getting required keys for guideline {self.id}: {e}")
             self._data_keys = [] # Default to empty if error


    def get_required_data_keys(self) -> List[str]:
        """Returns the list of data keys required for this guideline's calculation and suggestion."""
        # Consider caching this if params don't change, or re-calculating if they can
        # Re-calculate in case params influence keys (though get_required_data_keys takes params)
        try:
            return self.value_suggester.get_required_data_keys(self.params)
        except Exception as e:
            print(f"Error getting required keys for guideline {self.id}: {e}")
            return []


    def check(self, data: Dict[str, Any]) -> GuidelineResult:
        """Executes the guideline check using injected components."""
        value_to_check = None
        calc_error_msg = None
        suggestion = None
        details = None
        overall_error_msg = None

        try:
            # --- Step 1: Calculate Value ---
            value_to_check, calc_error_msg = self.value_suggester.calculate_value(data, self.params)

            # --- Step 2: Perform Check ---
            if calc_error_msg:
                 # If calculation failed, create CheckDetails reflecting this
                 details = CheckDetails(is_breached=True, actual_value=None,
                                        target_value=self.params.get('target'),
                                        lower_limit=self.params.get('lower_limit'),
                                        upper_limit=self.params.get('upper_limit'),
                                        error_message=f"Calculation Error: {calc_error_msg}")
            else:
                 # Calculation succeeded (or returned a value), proceed to check it
                 details = self.checker.check(self.params, value_to_check)

            # --- Step 3: Calculate Suggestion ---
            # Always attempt suggestion calculation, passing the CheckDetails (which might contain errors).
            # The suggester should handle cases where details indicate an error.
            suggestion = self.value_suggester.calculate_suggestion(details, data, self.params)

        except Exception as e:
            # Catch unexpected errors during the orchestration process
            print(f"CRITICAL ERROR during guideline check for {self.id}: {e}")
            overall_error_msg = f"Unexpected Orchestration Error: {e}"
            # Create a minimal error result if a full 'details' object wasn't created
            if details is None:
                 details = CheckDetails(is_breached=True, actual_value=value_to_check,
                                        target_value=self.params.get('target'), lower_limit=self.params.get('lower_limit'),
                                        upper_limit=self.params.get('upper_limit'), error_message=overall_error_msg)
            # Ensure suggestion reflects the critical error
            suggestion = ActionSuggestion(f"Guideline processing failed: {overall_error_msg}", severity="Error")


        # --- Step 4: Assemble Final Result ---
        # Use details if available, otherwise create default error state
        if details is None:
             # This should only happen if the initial try block failed very early
             details = CheckDetails(is_breached=True, actual_value=None, target_value=self.params.get('target'),
                                    lower_limit=self.params.get('lower_limit'), upper_limit=self.params.get('upper_limit'),
                                    error_message=overall_error_msg or "Unknown error before check details generation")
             if suggestion is None: suggestion = ActionSuggestion("Guideline processing failed critically", severity="Error")


        final_error_msg = details.error_message or overall_error_msg # Prioritize specific error

        return GuidelineResult(
            guideline_id=self.id,
            is_breached=details.is_breached,
            actual_value=details.actual_value,
            target_value=details.target_value,
            lower_limit=details.lower_limit,
            upper_limit=details.upper_limit,
            check_data=data, # Include the raw data for context/auditing
            suggestion=suggestion,
            error_message=final_error_msg
        )


# --- Example Setup ---

# Dummy Data Resolver & Data
class DummyResolver:
    def __init__(self, data):
        self._data = data
    def get_data(self, keys: List[str]) -> Dict[str, Any]:
        print(f"Resolver fetching keys: {keys}")
        # In reality, fetch only requested keys. Here, return all for simplicity.
        return self._data

raw_portfolio_data = {
    # Holdings structure assuming a list of dicts
    'holdings': [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}, {'ticker': 'GOOG'}, {'ticker': 'TBond'}],
    # Individual asset data using :qty, :price, :class convention
    'AAPL:qty': 10.0, 'AAPL:price': 175.0, 'AAPL:class': 'US Equity',
    'MSFT:qty': 5.0,  'MSFT:price': 300.0, 'MSFT:class': 'US Equity',
    'GOOG:qty': 8.0,  'GOOG:price': 140.0, 'GOOG:class': 'US Equity',
    'TBond:qty': 20.0, 'TBond:price': 98.0,  'TBond:class': 'US Bond',
    'Cash': 500.0 # Example, not directly used by TotalPortfolioValueCalculator
}
# Manual verification:
# AAPL Value = 10.0 * 175.0 = 1750.0
# MSFT Value = 5.0 * 300.0 = 1500.0
# GOOG Value = 8.0 * 140.0 = 1120.0
# TBond Value = 20.0 * 98.0 = 1960.0
# Total Portfolio Value = 1750 + 1500 + 1120 + 1960 = 6330.0

resolver = DummyResolver(raw_portfolio_data)

# --- Instantiate Components ---

# Calculators (shared instances are fine if stateless)
total_value_calc = TotalPortfolioValueCalculator()
stock_value_calc = SingleStockValueCalculator()
asset_class_value_calc = AssetClassValueCalculator()

# Value Suggesters (using composition)
# Suggester for single stock allocation vs total portfolio
stock_allocation_suggester = RatioValueSuggester(
    numerator_calculator=stock_value_calc,
    denominator_calculator=total_value_calc,
    rounding_precision=2 # Suggest quantity rounded to 2 decimal places
)
# Suggester for asset class allocation vs total portfolio
asset_class_allocation_suggester = RatioValueSuggester(
    numerator_calculator=asset_class_value_calc,
    denominator_calculator=total_value_calc,
    # Suggestion for asset class might be value-based if no price key given
    rounding_precision=0 # Or higher if suggesting units of a representative ETF
)

# Checker (shared instance is fine)
range_checker = FloatRangeChecker()

# --- Create Guidelines ---

guideline1 = Guideline(
    id="AAPL_Alloc_Limit",
    value_suggester=stock_allocation_suggester, # Use the stock allocation suggester
    checker=range_checker,
    params={
        # Params for RatioValueSuggester -> SingleStockNumeratorCalculator
        'target_identifier': 'AAPL',
        # Params for FloatRangeChecker
        'lower_limit': 0.20,
        'upper_limit': 0.30,
        'target': 0.25
    }
)

guideline2 = Guideline(
    id="US_Equity_Alloc_Limit",
    value_suggester=asset_class_allocation_suggester, # Use the asset class suggester
    checker=range_checker,
    params={
        # Params for RatioValueSuggester -> AssetClassNumeratorCalculator
        'target_identifier': 'US Equity',
        # 'representative_price_key': 'VOO:price', # Optional: needed for quantity suggestion
        # Params for FloatRangeChecker
        'lower_limit': 0.60,
        'upper_limit': 0.80,
        'target': 0.70
    }
)

guideline3 = Guideline(
    id="MSFT_Alloc_Breach", # Expecting a breach here
    value_suggester=stock_allocation_suggester, # Reuse the stock suggester
    checker=range_checker,
    params={
        'target_identifier': 'MSFT',
        'lower_limit': 0.238, # Set limits to cause breach for MSFT (0.237)
        'upper_limit': 0.239, # Or check > upper: 0.236, 0.237
        'target': 0.25      # Target is still higher
    }
)

# --- Run Checks ---

all_guidelines = [guideline1, guideline2, guideline3]
all_results = []

# 1. Get all required keys (aggregate from all guidelines)
print("\n--- Aggregating Required Keys ---")
all_keys = set()
for g in all_guidelines:
    keys_for_g = g.get_required_data_keys()
    print(f"Guideline '{g.id}' requires: {keys_for_g}")
    all_keys.update(keys_for_g)
print(f"All unique keys required: {list(all_keys)}")


# 2. Fetch data once
print("\n--- Fetching Data ---")
resolved_data = resolver.get_data(list(all_keys)) # Pass the actual keys needed


# 3. Check each guideline
print("\n--- Checking Guidelines ---")
for g in all_guidelines:
    print(f"\nChecking Guideline: {g.id}")
    result = g.check(resolved_data)
    all_results.append(result)
    # Print result with more detail for verification
    print(f"  Result: {result.is_breached} ({'BREACHED' if result.is_breached else 'OK'})")
    print(f"  Actual Value: {result.actual_value}")
    print(f"  Limits: [{result.lower_limit}, {result.upper_limit}], Target: {result.target_value}")
    print(f"  Suggestion: {result.suggestion}")
    if result.error_message:
        print(f"  Error Message: {result.error_message}")


# --- Analysis (Based on Manual Verification) ---
# Total Value = 6330.0
# AAPL Alloc = 1750.0 / 6330.0 = 0.2764... (Expected: OK [0.20, 0.30], Suggest Sell towards 0.25)
# US Equity Alloc = (1750 + 1500 + 1120) / 6330 = 4370 / 6330 = 0.6903... (Expected: OK [0.60, 0.80], Suggest Buy towards 0.70)
# MSFT Alloc = 1500 / 6330 = 0.2371... (Expected: BREACHED [0.238, 0.239] - below lower limit, Suggest Buy towards 0.238)