from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, List

# Assume date_lib exists and has a function like:
# date_lib.generate_schedule(rule_type: str, start_date: date, end_date: date, **kwargs) -> List[date]
# Or specific functions per rule type.
# Also assume a Resolver class like before.

# --- Strategy Interface ---


class IScheduleGenerator(ABC):
    @abstractmethod
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        """Declare data keys needed from the resolver."""
        pass

    @abstractmethod
    def generate(
        self, params: Dict[str, Any], resolved_data: Dict[str, Any]
    ) -> List[date]:
        """Generate the schedule based on params and resolved data."""
        pass


# --- Concrete Strategies ---


class NthBusinessDayGenerator(IScheduleGenerator):
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        keys = ["schedule_start_date", "schedule_end_date"]
        if "calendar_code_key" in params:
            keys.append(params["calendar_code_key"])
        # Add other keys if needed based on date_lib requirements
        return keys

    def generate(
        self, params: Dict[str, Any], resolved_data: Dict[str, Any]
    ) -> List[date]:
        n = params.get("n")
        from_end = params.get("from_end", False)
        calendar_code = resolved_data.get(
            params.get("calendar_code_key")
        )  # Get calendar from resolved data
        start_date = resolved_data.get("schedule_start_date")
        end_date = resolved_data.get("schedule_end_date")

        if n is None or start_date is None or end_date is None:
            # Handle missing essential parameters/data
            print("Error: Missing parameters/data for NthBusinessDayGenerator")
            return []

        try:
            # --- Adapt parameters for date_lib ---
            # Example: Assuming date_lib needs specific keyword args
            schedule = date_lib.generate_schedule(
                rule_type="nth_business_day",
                start_date=start_date,
                end_date=end_date,
                n=n,
                from_end=from_end,
                calendar=calendar_code,
                # Add other necessary args for date_lib
            )
            return schedule
        except Exception as e:
            print(f"Error calling date_lib for NthBusinessDay: {e}")
            return []  # Return empty list on error


# --- Nth Weekday Generator ---
class NthWeekdayGenerator(IScheduleGenerator):
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        keys = ["schedule_start_date", "schedule_end_date"]
        if "calendar_code_key" in params:
            keys.append(params["calendar_code_key"])
        return keys

    def generate(
        self, params: Dict[str, Any], resolved_data: Dict[str, Any]
    ) -> List[date]:
        n = params.get("n")
        weekday = params.get("weekday")  # e.g., 'Friday', 4 (0=Mon)
        calendar_code = resolved_data.get(params.get("calendar_code_key"))
        start_date = resolved_data.get("schedule_start_date")
        end_date = resolved_data.get("schedule_end_date")

        if n is None or weekday is None or start_date is None or end_date is None:
            print("Error: Missing parameters/data for NthWeekdayGenerator")
            return []

        try:
            # --- Adapt parameters for date_lib ---
            schedule = date_lib.generate_schedule(
                rule_type="nth_weekday",
                start_date=start_date,
                end_date=end_date,
                n=n,
                weekday=weekday,  # Ensure date_lib understands the format
                calendar=calendar_code,
            )
            return schedule
        except Exception as e:
            print(f"Error calling date_lib for NthWeekday: {e}")
            return []


# --- Manual Date Generator ---
class ManualDateGenerator(IScheduleGenerator):
    def get_required_data_keys(self, params: Dict[str, Any]) -> List[str]:
        date_list_key = params.get("date_list_key")
        return [date_list_key] if date_list_key else []

    def generate(
        self, params: Dict[str, Any], resolved_data: Dict[str, Any]
    ) -> List[date]:
        date_list_key = params.get("date_list_key")
        if not date_list_key:
            print("Error: Missing 'date_list_key' in params for ManualDateGenerator")
            return []

        raw_dates = resolved_data.get(date_list_key)
        if not isinstance(raw_dates, list):
            print(f"Error: Data for key '{date_list_key}' is not a list or missing.")
            return []

        parsed_dates: List[date] = []
        for d_str in raw_dates:
            try:
                # Assuming dates are strings like 'YYYY-MM-DD'
                # Add more robust parsing if needed
                parsed_dates.append(date.fromisoformat(str(d_str)))
            except (ValueError, TypeError) as e:
                print(
                    f"Warning: Could not parse date '{d_str}' from list '{date_list_key}': {e}"
                )
                # Decide: skip invalid date or return error? Skipping for now.

        # Optional: Sort the dates
        parsed_dates.sort()
        return parsed_dates


# --- Nth Calendar Day Generator ---
# Similar structure, adapting params for date_lib's calendar day rule + roll adjustments


# --- Event Class ---


class Event:
    def __init__(
        self, event_id: str, generator: IScheduleGenerator, params: Dict[str, Any]
    ):
        self.event_id = event_id
        if not isinstance(generator, IScheduleGenerator):
            raise TypeError("generator must implement IScheduleGenerator")
        self.generator = generator
        self.params = params  # Rule-specific params (N, day, keys etc)

    def get_required_data_keys(self) -> List[str]:
        """Get data keys required by the assigned generator."""
        try:
            return self.generator.get_required_data_keys(self.params)
        except Exception as e:
            print(f"Error getting required keys for event {self.event_id}: {e}")
            return []

    def generate_schedule(self, resolver: "Resolver") -> List[date]:
        """Generates the event schedule using its strategy and the resolver."""
        schedule: List[date] = []
        required_keys = self.get_required_data_keys()
        if not required_keys:
            # Handle cases where keys couldn't be determined or aren't needed (rare)
            print(f"Warning: No required keys determined for event {self.event_id}.")
            # Proceed assuming no external data needed beyond params? Or fail?
            # Let's try generating with empty resolved_data, strategy must handle it.
            resolved_data = {}
        else:
            try:
                resolved_data = resolver.get_data(required_keys)
            except Exception as e:
                print(f"Error resolving data for event {self.event_id}: {e}")
                return []  # Cannot proceed without resolved data

        try:
            # Delegate generation to the strategy
            schedule = self.generator.generate(self.params, resolved_data)
            return schedule
        except Exception as e:
            # Catch unexpected errors during generation
            print(
                f"CRITICAL Error during schedule generation for event {self.event_id}: {e}"
            )
            return []


# --- Optional: Event Factory ---
# If creating events with correct generator/params is complex


class EventFactory:
    def create_event(self, config: Dict[str, Any]) -> Event:
        event_id = config.get("event_id")
        schedule_rule = config.get("schedule_rule")
        params = config.get("params", {})

        generator: IScheduleGenerator
        if schedule_rule == "nth_business_day":
            generator = NthBusinessDayGenerator()
            # Validate required params like 'n' exist?
        elif schedule_rule == "nth_weekday":
            generator = NthWeekdayGenerator()
            # Validate required params like 'n', 'weekday' exist?
        elif schedule_rule == "manual":
            generator = ManualDateGenerator()
            # Validate required params like 'date_list_key' exist?
        # Add other rules...
        else:
            raise ValueError(f"Unknown schedule_rule: {schedule_rule}")

        if not event_id:
            raise ValueError("Missing event_id in config")

        return Event(event_id, generator, params)
