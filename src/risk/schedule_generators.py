# schedule_generators.py
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

# Import interface and base class
from .interfaces import IScheduleGenerator

log = logging.getLogger(__name__)

# Attempt to import the date library, provide a dummy if not found
try:
    # Replace 'date_lib' with the actual name of your date calculation library
    import date_lib  # <--- CHANGE THIS TO YOUR ACTUAL LIBRARY

    log.info("External date library 'date_lib' loaded.")
except ImportError:
    log.warning(
        "Dummy 'date_lib' will be used as the real one was not found. Schedule generation may be limited."
    )

    # Create a dummy object/module that mimics the expected API
    class DummyDateLib:
        def generate_schedule(self, rule_type, start_date, end_date, **kwargs):
            log.warning(
                f"DummyDateLib.generate_schedule called for rule '{rule_type}'. Returning empty list."
            )
            # Return a dummy schedule or raise an error
            return []

    date_lib = DummyDateLib()


class NthBusinessDayGenerator(IScheduleGenerator):
    """Generates schedule based on the Nth business day rule."""

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        # Keys needed for date range and potentially calendar lookup
        keys = ["schedule_start_date", "schedule_end_date"]
        if "calendar_code_key" in params:  # Key to lookup the calendar identifier
            keys.append(params["calendar_code_key"])
        log.debug(f"NthBusinessDayGenerator requires keys: {keys} for params: {params}")
        return keys

    async def generate(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[List[date], Optional[str]]:
        if date_lib is None or isinstance(
            date_lib, DummyDateLib
        ):  # Check if real lib loaded
            return [], "Date calculation library not available"

        n = params.get("n")
        from_end = params.get("from_end", False)
        calendar_key = params.get("calendar_code_key")
        # Use default calendar if key/value not found? Or make required?
        calendar_code = (
            resolved_data.get(calendar_key, "DEFAULT_CALENDAR")
            if calendar_key
            else "DEFAULT_CALENDAR"
        )
        start_date = resolved_data.get("schedule_start_date")
        end_date = resolved_data.get("schedule_end_date")

        # Validate parameters
        if n is None or not isinstance(n, int):
            return [], "Missing or invalid 'n' parameter"
        if start_date is None or not isinstance(start_date, date):
            return [], "Missing or invalid 'schedule_start_date'"
        if end_date is None or not isinstance(end_date, date):
            return [], "Missing or invalid 'schedule_end_date'"
        if start_date > end_date:
            return [], "'schedule_start_date' cannot be after 'schedule_end_date'"

        log.debug(
            f"Generating Nth Business Day: n={n}, from_end={from_end}, calendar={calendar_code}, "
            f"period={start_date} to {end_date}"
        )
        try:
            # --- Call the actual date library ---
            # Adjust the call based on your library's API
            # If the library call is blocking, wrap it in asyncio.to_thread
            # schedule = await asyncio.to_thread(date_lib.generate_schedule, ...)
            # Assuming a synchronous library for now:
            schedule = date_lib.generate_schedule(  # Use the real or dummy library
                rule_type="nth_business_day",
                start_date=start_date,
                end_date=end_date,
                n=n,
                from_end=from_end,
                calendar=calendar_code,  # Pass calendar identifier
                # Add any other required arguments for your library
            )
            # ------------------------------------
            # Ensure returned values are date objects
            valid_schedule = [d for d in schedule if isinstance(d, date)]
            if len(valid_schedule) != len(schedule):
                log.warning(
                    "Some non-date objects returned by date_lib were filtered out."
                )

            log.debug(f"Generated {len(valid_schedule)} dates.")
            return valid_schedule, None  # Success
        except Exception as e:
            log.error(
                f"Error calling date_lib for Nth Business Day schedule: {e}",
                exc_info=True,
            )
            return [], f"Failed to generate schedule via date_lib: {e}"


class ManualDateGenerator(IScheduleGenerator):
    """Uses a manually defined list of dates."""

    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        date_list_key = params.get("date_list_key")
        log.debug(f"ManualDateGenerator requires key: {date_list_key}")
        return [date_list_key] if date_list_key else []

    async def generate(
        self, resolved_data: Dict[str, Any], params: Dict[str, Any]
    ) -> Tuple[List[date], Optional[str]]:
        date_list_key = params.get("date_list_key")
        if not date_list_key:
            return [], "Missing 'date_list_key' parameter"

        raw_dates = resolved_data.get(date_list_key)
        if raw_dates is None:
            return [], f"Data not found for key '{date_list_key}'"
        if not isinstance(raw_dates, list):
            return (
                [],
                f"Data for key '{date_list_key}' is not a list, type is {type(raw_dates).__name__}",
            )

        log.debug(
            f"Generating manual schedule from key '{date_list_key}' with {len(raw_dates)} items."
        )
        parsed_dates: List[date] = []
        errors = []
        for i, d_val in enumerate(raw_dates):
            try:
                if isinstance(d_val, date):  # Already a date object
                    parsed_dates.append(d_val)
                elif isinstance(d_val, str):  # Attempt to parse ISO format string
                    parsed_dates.append(date.fromisoformat(d_val))
                else:
                    raise ValueError(f"Unsupported type {type(d_val).__name__}")
            except (ValueError, TypeError) as e:
                err_msg = f"Invalid date value at index {i}: '{d_val}' ({e})"
                log.warning(err_msg)
                errors.append(err_msg)
                # Continue processing other dates

        if errors:
            # Decide: Return partially parsed dates with error, or fail completely?
            # Failing completely might be safer.
            error_summary = f"Errors parsing dates: {'; '.join(errors)}"
            log.error(error_summary)
            return [], error_summary

        parsed_dates.sort()  # Sort the dates chronologically
        log.debug(f"Generated {len(parsed_dates)} dates successfully.")
        return parsed_dates, None


# --- Implement other generators (NthWeekdayGenerator, etc.) similarly ---
# Remember to adapt the calls to your specific 'date_lib' API.
