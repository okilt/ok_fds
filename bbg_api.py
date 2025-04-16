# -*- coding: utf-8 -*-
import blpapi
import time
import threading
import queue
import logging
import configparser
import os
from collections import defaultdict
from datetime import datetime
import json
import asyncio # For asyncio integration

# --- Constants Definition ---
# blpapi.Name objects (pre-generating them is efficient)
SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SESSION_TERMINATED      = blpapi.Name("SessionTerminated")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
SLOW_CONSUMER_WARNING   = blpapi.Name("SlowConsumerWarning")
SLOW_CONSUMER_WARNING_CLEARED = blpapi.Name("SlowConsumerWarningCleared")
DATA_LOSS               = blpapi.Name("DataLoss")
REQUEST_FAILURE         = blpapi.Name("RequestFailure")
ADMIN                   = blpapi.Name("Admin")
TIMEOUT                 = blpapi.Name("Timeout") # Name constant for timeout category/reason

# Event Types (from blpapi.Event)
PARTIAL_RESPONSE        = blpapi.Event.EventType.PARTIAL_RESPONSE
RESPONSE                = blpapi.Event.EventType.RESPONSE
TIMEOUT_EVENT           = blpapi.Event.EventType.TIMEOUT # Event type for timeout from nextEvent
ADMIN_EVENT             = blpapi.Event.EventType.ADMIN
SESSION_STATUS          = blpapi.Event.EventType.SESSION_STATUS
SERVICE_STATUS          = blpapi.Event.EventType.SERVICE_STATUS
# Many other event types exist

# Message Types (Used in Admin events, etc.)
PERMISSION_REQUEST      = blpapi.Name("PermissionRequest")
RESOLUTION_SUCCESS      = blpapi.Name("ResolutionSuccess")
RESOLUTION_FAILURE      = blpapi.Name("ResolutionFailure")
# Many others

# Data Element Names
SECURITY_DATA           = blpapi.Name("securityData")
SECURITY_NAME           = blpapi.Name("security")
FIELD_DATA              = blpapi.Name("fieldData")
FIELD_EXCEPTIONS        = blpapi.Name("fieldExceptions")
FIELD_ID                = blpapi.Name("fieldId")
ERROR_INFO              = blpapi.Name("errorInfo")
MESSAGE                 = blpapi.Name("message")
CATEGORY                = blpapi.Name("category")
SUBCATEGORY             = blpapi.Name("subcategory")
CODE                    = blpapi.Name("code")
SOURCE                  = blpapi.Name("source")
SECURITY_ERROR          = blpapi.Name("securityError")
RESPONSE_ERROR          = blpapi.Name("responseError")
REASON                  = blpapi.Name("reason") # Within RequestFailure

# Intraday Data Names
BAR_DATA                = blpapi.Name("barData")
BAR_TICK_DATA           = blpapi.Name("barTickData")
TICK_DATA               = blpapi.Name("tickData") # For IntradayTickResponse container
TIME                    = blpapi.Name("time")
OPEN                    = blpapi.Name("open")
HIGH                    = blpapi.Name("high")
LOW                     = blpapi.Name("low")
CLOSE                   = blpapi.Name("close")
VOLUME                  = blpapi.Name("volume")
NUM_EVENTS              = blpapi.Name("numEvents")
VALUE                   = blpapi.Name("value") # For Tick data price/value
TYPE                    = blpapi.Name("type") # For Tick data type (TRADE, BID, ASK)

# --- Error Categories (For retry logic) ---
# These are examples and need adjustment based on actual category/subcategory strings
RETRYABLE_CATEGORIES = {"TIMEOUT", "NETWORK_ERROR", "SERVER_ERROR", "CONNECTION_FAILURE"} # Hypothetical category names
NON_RETRYABLE_CATEGORIES = {"BAD_SECURITY", "BAD_FIELD", "AUTHORIZATION_FAILURE", "INVALID_REQUEST", "ILLEGAL_FIELD"} # Hypothetical category names


# --- Logger Setup ---
def setup_logger(name='blpapi_wrapper', level=logging.INFO, log_file=None):
    """Helper function to set up a logger."""
    logger = logging.getLogger(name)
    # Avoid adding handlers multiple times (prevents duplicate logs)
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler (if specified)
    if log_file:
        try:
            fh = logging.FileHandler(log_file, encoding='utf-8')
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        except Exception as e:
            print(f"Warning: Could not create log file handler for {log_file}. Error: {e}")


    # Adjust the log level of the blpapi library itself (if too verbose)
    blpapi_logger = logging.getLogger('blpapi')
    blpapi_logger.setLevel(logging.WARNING) # Change as needed

    return logger

# --- Configuration Management ---
def load_config(config_file='config.ini'):
    """Loads connection info, etc., from a configuration file."""
    config = configparser.ConfigParser()
    # Default values
    defaults = {
        'host': 'localhost',
        'port': '8194',
        'timeout': '30',
        'max_retries': '3',
        'retry_delay': '2',
        'log_level': 'INFO',
        'log_file': '' # Empty means no file logging
    }
    # Environment variable overrides (example)
    defaults['host'] = os.environ.get('BLPAPI_HOST', defaults['host'])
    defaults['port'] = os.environ.get('BLPAPI_PORT', defaults['port'])
    # Add others similarly

    config['DEFAULT'] = defaults # Set defaults in the parser

    if os.path.exists(config_file):
        try:
            config.read(config_file, encoding='utf-8')
            print(f"Loaded configuration from {config_file}")
        except Exception as e:
            print(f"Warning: Could not read config file {config_file}. Using defaults. Error: {e}")
    else:
        print(f"Warning: Config file {config_file} not found. Using defaults.")

    # Return the loaded settings (with appropriate type conversions)
    settings = {
        'host': config['DEFAULT'].get('host'),
        'port': config['DEFAULT'].getint('port'), # Convert to int
        'timeout': config['DEFAULT'].getint('timeout'),
        'max_retries': config['DEFAULT'].getint('max_retries'),
        'retry_delay': config['DEFAULT'].getfloat('retry_delay'), # Convert to float
        'log_level': config['DEFAULT'].get('log_level').upper(), # To uppercase
        'log_file': config['DEFAULT'].get('log_file') or None # Empty string becomes None
    }
    return settings


class BlpApiWrapper:
    """
    Bloomberg API Asynchronous Wrapper (Enhanced for Production)
    - Detailed error handling and logging
    - Configuration management
    - Intraday data support
    - Enhanced thread safety
    - Excel-like simple interface
    """
    def __init__(self, config_file='config.ini'):
        self.settings = load_config(config_file)
        self.logger = setup_logger(
            level=getattr(logging, self.settings['log_level'], logging.INFO),
            log_file=self.settings['log_file']
        )

        self.host = self.settings['host']
        self.port = self.settings['port']
        self.default_timeout = self.settings['timeout']
        self.default_max_retries = self.settings['max_retries']
        self.default_retry_delay = self.settings['retry_delay']

        self.session = None
        self.session_started_event = threading.Event()
        self.session_status = "stopped"
        self.session_status_lock = threading.Lock()
        self.service_states = {} # key: service_name, value: 'opening', 'opened', or 'failed'
        self.service_states_lock = threading.Lock() # Lock for accessing service states dictionary

        self.response_data = defaultdict(lambda: {"status": "pending", "data": [], "errors": []})
        self.response_data_lock = threading.Lock() # Lock for accessing response data dictionary

        self.event_thread = None
        self.shutdown_event = threading.Event()
        self.next_correlation_id = 0
        self.cid_lock = threading.Lock() # Lock for generating Correlation IDs

        self.identity = None # Used if authorization is required

        self.logger.info(f"BlpApiWrapper initialized with settings: {self.settings}")

    def _get_next_correlation_id(self):
        """Generates the next Correlation ID safely."""
        with self.cid_lock:
            self.next_correlation_id += 1
            # Create a CorrelationId object
            return blpapi.CorrelationId(self.next_correlation_id)

    def start_session(self, auth_options=None):
        """
        Starts the session and the event processing thread.
        Optionally provides authorization information.
        :param auth_options: Dictionary with authorization details (e.g., {'token': '...'} or None).
        :return: True if successful, False otherwise.
        """
        if self.session:
            self.logger.warning("Session already started.")
            return True

        session_options = blpapi.SessionOptions()
        session_options.setServerHost(self.host)
        session_options.setServerPort(self.port)
        # session_options.setAutoRestartOnDisconnection(True) # Auto-restart option

        with self.session_status_lock:
            if self.session:
                self.logger.warning("Session already started or starting.")
                # Optionally check self.session_status if finer control is needed
                return self.session_status == "started"
            self.session_status = "starting" # Mark as starting
        
        self.logger.info(f"Attempting to connect to {self.host}:{self.port}")
        self.session_started_event.clear() # Ensure event is clear before starting
        # Create session with the event handler
        self.session = blpapi.Session(session_options, self._event_handler)
    
        if not self.session.start():
            self.logger.error("Failed to initiate session start sequence (session.start() returned False).")
            with self.session_status_lock:
                self.session_status = "failed_to_start"
            self.session = None
            return False

        self.logger.info("Session start initiated. Starting event loop and waiting for SessionStarted/Failure event...")

        # Start the event processing thread
        self.shutdown_event.clear()
        self.event_thread = threading.Thread(target=self._event_loop, name="BlpapiEventThread")
        self.event_thread.daemon = True # Allow program to exit even if this thread is running
        self.event_thread.start()

        # --- Wait for SessionStarted or SessionStartupFailure ---
        SESSION_START_TIMEOUT = 15 # seconds
        event_set = self.session_started_event.wait(timeout=SESSION_START_TIMEOUT)
    
        # Check the final status determined by the event handler
        with self.session_status_lock:
            final_status = self.session_status
    
        if event_set and final_status == "started":
            self.logger.info("Session confirmed started successfully.")
            # Authorization step (if needed) should come AFTER session is confirmed started
            if auth_options:
                self.logger.info("Attempting authorization...")
                if not self._authorize(auth_options):
                    self.logger.error("Authorization failed.")
                    self.stop_session() # Stop session if auth fails
                    return False
                self.logger.info("Authorization successful.")
            else:
                self.logger.info("No authorization required or identity will be obtained later.")
            return True
        elif final_status == "failed_to_start":
             # This status would be set if the event handler received SessionStartupFailure
             self.logger.error("Session startup failed (detected by event handler).")
             self.stop_session() # Clean up
             return False
        elif not event_set:
            # Timeout occurred
            self.logger.error(f"Session start timed out after {SESSION_START_TIMEOUT} seconds. Event loop might be stuck or connection failed silently.")
            # Attempt to stop the session cleanly, although it might be in a bad state
            self.stop_session()
            return False
        else:
            # Event was set, but status is not "started" (e.g., still "starting" or unexpected)
            self.logger.error(f"Session start event was set, but final status is unexpected: '{final_status}'.")
            self.stop_session()
            return False

    def _authorize(self, auth_options):
        """Handles the authorization process."""
        if not self.session:
            self.logger.error("Session not started, cannot authorize.")
            return False

        # Correlation ID for authorization
        auth_cid = self.session.generateCorrelationId()

        # Open the authorization service
        auth_service_name = "//blp/apiauth"
        if not self.session.openService(auth_service_name):
            self.logger.error(f"Failed to open {auth_service_name} service.")
            return False
        auth_service = self.session.getService(auth_service_name)
        if not auth_service:
             self.logger.error(f"Could not get {auth_service_name} service object after opening.")
             return False


        # Create authorization request
        auth_request = auth_service.createAuthorizationRequest()

        # Set options based on the provided dictionary
        if 'token' in auth_options:
            auth_request.set("token", auth_options['token'])
            self.logger.info("Using token-based authorization.")
        elif 'userAndApp' in auth_options:
            # User/App authentication (SAPI/B-PIPE) is complex.
            # It's often better handled via SessionOptions or specific Identity creation methods.
            # This example assumes token auth is the primary method here.
            self.logger.error("User/App auth via request is not fully implemented here. Prefer setting in SessionOptions or use specific Identity methods if needed.")
            # Example structure (might need adjustment):
            # auth_user = self.session.createIdentityUserInfo()
            # auth_user.setUserName(...)
            # auth_user.setApplicationName(...)
            # self.identity = self.session.createIdentity(auth_user)
            # Or use session_options.setSessionIdentityOptions(...) before session.start()
            return False # Indicate failure for this simplified example
        else:
            self.logger.error(f"Unsupported auth_options type: {list(auth_options.keys())}")
            return False

        # Send the authorization request (Identity is usually None here)
        self.session.sendAuthorizationRequest(auth_request, None, auth_cid)
        self.logger.info(f"Sent authorization request with CID: {auth_cid.value()}")

        # Wait for the authorization result (processed in the event loop)
        start_time = time.time()
        timeout = 30 # Authorization timeout
        auth_successful = False
        while time.time() - start_time < timeout:
            # Use nextEvent to process events, including the auth response
            event = self.session.nextEvent(timeout=1000) # Check every second

            if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.REQUEST_STATUS:
                for msg in event:
                    # Check if this message corresponds to our auth request
                    if msg.correlationIds() and msg.correlationIds()[0] == auth_cid:
                        if msg.messageType() == blpapi.Name("AuthorizationSuccess"):
                            self.logger.info("AuthorizationSuccess received.")
                            # For SAPI/B-PIPE, you might get an Identity object here.
                            # For Desktop API, authorization is often implicit, but you might
                            # still need to create a default Identity if making requests requires one.
                            # self.identity = self.session.createIdentity() # May be needed depending on setup
                            self.logger.info("Identity established (specifics depend on auth type and API setup).")
                            auth_successful = True
                            break # Exit message loop
                        elif msg.messageType() == blpapi.Name("AuthorizationFailure"):
                            self.logger.error(f"AuthorizationFailure received: {msg}")
                            if msg.hasElement(REASON):
                                reason = msg.getElement(REASON)
                                self.logger.error(f"Reason: {self._extract_error_info(reason)}")
                            auth_successful = False
                            break # Exit message loop
                        else:
                            self.logger.warning(f"Received unexpected message type during auth: {msg.messageType()}")
                if auth_successful is not None: # If we processed the auth response
                    break # Exit waiting loop
            elif event.eventType() == blpapi.Event.TIMEOUT:
                 self.logger.debug("Timeout waiting for authorization response event.")
                 continue # Continue waiting
            else:
                 # Process other events (like SERVICE_STATUS) that might occur during auth wait
                 self.logger.debug(f"Received other event type {event.eventType()} during auth wait.")
                 self._process_admin_or_status_event(event)


        if auth_successful:
            return True
        elif auth_successful is False: # Explicit failure
             return False
        else: # Timeout
             self.logger.error("Authorization timed out.")
             return False


    def stop_session(self):
        """Stops the session and terminates the event thread."""
        if not self.session:
            self.logger.info("Session already stopped or not started.")
            return

        self.logger.info("Stopping session...")
        self.shutdown_event.set() # Signal the event loop to terminate

        # Stop the session (this releases the event queue)
        # stop() can be synchronous or asynchronous. Try sync first.
        try:
            # Using SYNC ensures stop completes before proceeding, but can block
            self.session.stop(blpapi.Session.StopOption.SYNC)
            self.logger.info("Session stop (SYNC) completed.")
        except Exception as e:
            self.logger.warning(f"Exception during SYNC session stop: {e}. Trying ASYNC.")
            # If sync fails or hangs, try async (less guarantee of completion order)
            try:
                 self.session.stop(blpapi.Session.StopOption.ASYNC)
                 self.logger.info("Session stop (ASYNC) initiated.")
            except Exception as e_async:
                 self.logger.error(f"Exception during ASYNC session stop: {e_async}")

        # Wait for the event processing thread to finish (with timeout)
        if self.event_thread and self.event_thread.is_alive():
            self.logger.info("Waiting for event thread to finish...")
            self.event_thread.join(timeout=5.0) # Wait up to 5 seconds
            if self.event_thread.is_alive():
                self.logger.warning("Event thread did not finish cleanly after stop request.")

        self.session = None
        self.service_states = {} # Clear states
        self.identity = None # Clear identity
        self.logger.info("Session stopped.")

    def _open_service(self, service_name):
        """
        Opens the specified service synchronously, waiting for completion. Uses locks.
        :param service_name: The service name (e.g., "//blp/refdata").
        :return: True if the service is opened successfully, False otherwise.
        """
        # First check (outside main lock) for the common case where service is already open
        with self.service_states_lock:
            current_state = self.service_states.get(service_name)
            if current_state == 'opened':
                return True

        # If not opened, proceed with lock for state modification and check
        if not self.session:
            self.logger.error("Error: Session not started. Cannot open service.")
            return False

        needs_open_attempt = False
        with self.service_states_lock:
            current_state = self.service_states.get(service_name)
            if current_state == 'opened':
                return True # Opened by another thread while waiting for lock
            elif current_state == 'opening':
                self.logger.debug(f"Service {service_name} is already being opened by another thread. Waiting...")
                # We will wait outside the lock
            elif current_state == 'failed':
                 self.logger.warning(f"Service {service_name} previously failed to open. Retrying...")
                 # Reset state to allow retry
                 del self.service_states[service_name]
                 self.service_states[service_name] = 'opening'
                 needs_open_attempt = True
            else: # State is None (first attempt)
                 self.logger.info(f"Initiating open for service: {service_name}")
                 self.service_states[service_name] = 'opening'
                 needs_open_attempt = True

            # If we marked it for opening, make the API call now (still holding lock briefly)
            if needs_open_attempt:
                 if not self.session.openService(service_name):
                     self.logger.error(f"Failed to initiate opening service request: {service_name}")
                     self.service_states[service_name] = 'failed' # Update state under lock
                     return False

        # Wait for the service to open (outside the state modification lock)
        timeout = 20 # Service opening timeout
        start_time = time.time()
        opened_successfully = False
        while True:
            with self.service_states_lock:
                current_state = self.service_states.get(service_name)

            if current_state == 'opened':
                self.logger.info(f"Service {service_name} confirmed opened.")
                opened_successfully = True
                break
            if current_state == 'failed':
                self.logger.error(f"Service {service_name} failed to open (detected state change).")
                opened_successfully = False
                break
            # if current_state != 'opening': # Should not happen if logic is correct
            #     self.logger.error(f"Unexpected service state '{current_state}' for {service_name} while waiting.")
            #     opened_successfully = False
            #     break

            # Check timeout
            if time.time() - start_time > timeout:
                self.logger.error(f"Timeout waiting for service {service_name} to open after {timeout} seconds.")
                with self.service_states_lock:
                     # Ensure state is marked as failed on timeout if it was still 'opening'
                     if self.service_states.get(service_name) == 'opening':
                         self.service_states[service_name] = 'failed'
                opened_successfully = False
                break

            # Wait briefly for events to be processed by the event loop thread
            time.sleep(0.1)

        return opened_successfully

    def _event_loop(self):
        """The loop that processes events from the Bloomberg session queue (runs in a separate thread)."""
        self.logger.info("Event loop started.")
        while not self.shutdown_event.is_set():
            try:
                # Get the next event from the session's event queue (with timeout)
                event = self.session.nextEvent(timeout=500) # 500ms timeout

                # Branch processing based on event type
                event_type = event.eventType()

                if event_type == blpapi.Event.TIMEOUT:
                    # self.logger.debug("nextEvent timed out.")
                    continue # No event received within timeout, continue loop
                elif event_type in (PARTIAL_RESPONSE, RESPONSE):
                    # Handle data response events
                    for msg in event:
                        self._process_response_message(msg, event_type)
                elif event_type in (ADMIN_EVENT, SESSION_STATUS, SERVICE_STATUS):
                    # Handle administrative and status events
                    self._process_admin_or_status_event(event)
                # Add handling for other event types if needed (e.g., MARKET_DATA_EVENTS)
                # elif event_type == blpapi.Event.MARKET_DATA_EVENTS:
                #      self.logger.debug("Market data event received (subscription handling needed).")
                #      # Add subscription processing logic here if using mktdata service
                else:
                    # Log unknown or ignored event types for debugging
                    self.logger.debug(f"Ignoring event type: {event_type}")
                    # Optionally log message contents for unknown events
                    # for msg in event:
                    #     self.logger.debug(f"Ignored message content: {msg}")

            except Exception as e:
                # Catch unexpected errors within the event loop
                self.logger.exception(f"Critical error in event loop: {e}")
                # Depending on the error, consider session restart or shutdown
                # For now, log and continue, but could indicate a serious issue
                time.sleep(1) # Prevent tight loop on continuous errors

        self.logger.info("Event loop finished.")

    def _process_response_message(self, msg, event_type):
        """Processes response messages related to data requests."""
        cids = msg.correlationIds()
        if not cids:
            # Should not happen for request responses, but check defensively
            self.logger.warning(f"Received response message with no Correlation ID: MsgType={msg.messageType()}")
            return

        # Typically only one CID, but the API allows multiple
        for cid in cids:
            # Ensure it's a value-based CID (integer we assigned)
            if not cid.isObject():
                 cid_value = cid.value()
                 self.logger.debug(f"Processing response for CID: {cid_value}, EventType: {event_type}, MsgType: {msg.messageType()}")

                 # --- Access response data structure safely (using lock) ---
                 with self.response_data_lock:
                    if cid_value not in self.response_data:
                        # Could happen if request timed out and was already removed, or invalid CID
                        self.logger.warning(f"Received response for unknown or outdated CID: {cid_value}. Message Type: {msg.messageType()}. Ignoring.")
                        continue # Ignore this message

                    # Get the current state for this request
                    current_response = self.response_data[cid_value]

                    # If already completed or errored out, potentially a duplicate or late message
                    if current_response["status"] in ["complete", "error"]:
                         self.logger.debug(f"Ignoring message for already finalized CID: {cid_value} (Status: {current_response['status']})")
                         continue

                    # --- Parse message content ---
                    has_request_level_error = False

                    # 1. Check for request-level errors (affecting the whole request)
                    if msg.hasElement(RESPONSE_ERROR):
                        error_element = msg.getElement(RESPONSE_ERROR)
                        error_info = self._extract_error_info(error_element)
                        log_msg = f"CID {cid_value}: RequestError - Category: {error_info['category']}, SubCategory: {error_info['subcategory']}, Message: {error_info['message']}"
                        self.logger.error(log_msg)
                        current_response["errors"].append({"type": "RequestError", "details": error_info})
                        current_response["status"] = "error" # Mark request as failed
                        has_request_level_error = True

                    # Also check for RequestFailure message type (can indicate more severe issues)
                    if msg.messageType() == REQUEST_FAILURE:
                         if msg.hasElement(REASON):
                             reason = msg.getElement(REASON)
                             error_info = self._extract_error_info(reason)
                             log_msg = f"CID {cid_value}: RequestFailure - Category: {error_info['category']}, SubCategory: {error_info['subcategory']}, Message: {error_info['message']}"
                         else:
                              error_info = {"message": "No reason element found in RequestFailure"}
                              log_msg = f"CID {cid_value}: RequestFailure received with no reason element."
                         self.logger.error(log_msg)
                         current_response["errors"].append({"type": "RequestFailure", "details": error_info})
                         current_response["status"] = "error"
                         has_request_level_error = True

                    # 2. If no request-level error, extract data
                    if not has_request_level_error:
                        # Check for different data structures based on request type

                        # Reference Data or Historical Data Response
                        if msg.hasElement(SECURITY_DATA):
                            security_data_array = msg.getElement(SECURITY_DATA)
                            for sec_data_element in security_data_array.values(): # Iterate through security data blocks
                                parsed_sec_data = self._parse_security_data(sec_data_element, cid_value)
                                current_response["data"].append(parsed_sec_data)
                                # Add any security/field level errors found during parsing to the main error list
                                current_response["errors"].extend(parsed_sec_data.get("errors", []))

                        # Intraday Bar Response
                        elif msg.hasElement(BAR_DATA):
                            bar_data_element = msg.getElement(BAR_DATA)
                            if bar_data_element.hasElement(BAR_TICK_DATA):
                                bar_tick_data_array = bar_data_element.getElement(BAR_TICK_DATA)
                                for bar_tick_element in bar_tick_data_array.values(): # Iterate through bars
                                    parsed_bar = self._parse_bar_tick_data(bar_tick_element, cid_value)
                                    if parsed_bar: # Only add if parsing was successful
                                        current_response["data"].append(parsed_bar)
                            else:
                                self.logger.warning(f"CID {cid_value}: IntradayBarResponse received but no 'barTickData' element found.")

                        # Intraday Tick Response
                        elif msg.hasElement(TICK_DATA):
                            # Tick data might be nested within another TICK_DATA element
                            outer_tick_data = msg.getElement(TICK_DATA)
                            if outer_tick_data.hasElement(TICK_DATA):
                                tick_data_array = outer_tick_data.getElement(TICK_DATA)
                                for tick_element in tick_data_array.values(): # Iterate through ticks
                                     parsed_tick = self._parse_tick_data(tick_element, cid_value)
                                     if parsed_tick:
                                         current_response["data"].append(parsed_tick)
                            else:
                                 self.logger.warning(f"CID {cid_value}: IntradayTickResponse received but no nested 'tickData' element found.")

                        # Add handling for other response types (e.g., BQL) if needed in the future
                        # elif msg.messageType() == blpapi.Name("BqlResponse"):
                        #     # Add BQL parsing logic here
                        #     pass

                        elif event_type == RESPONSE and not current_response["data"] and not current_response["errors"]:
                             # If it's the final response but contains no known data structures and no errors were logged yet
                             # This might happen for requests that legitimately return no data (e.g., hist data for a future date)
                             # Or it could be an unhandled response format.
                             self.logger.info(f"CID {cid_value}: Final response received with no data or known errors. MessageType: {msg.messageType()}")
                             # You might want to log the raw message here for inspection:
                             # self.logger.debug(f"CID {cid_value}: Raw final empty message: {msg}")


                    # --- Update response status ---
                    # Only update status if not already marked as 'error'
                    if current_response["status"] != "error":
                        if event_type == RESPONSE:
                            current_response["status"] = "complete"
                            self.logger.info(f"CID {cid_value}: Response marked complete.")
                        elif event_type == PARTIAL_RESPONSE:
                            current_response["status"] = "partial"
                            # No need to log every partial response, could be verbose
                            self.logger.debug(f"CID {cid_value}: Partial response received.")

                 # --- Lock released ---
            else:
                 # CID is an object (e.g., for authorization responses handled elsewhere)
                 self.logger.debug(f"Ignoring message with object Correlation ID in response processor: {cid}")


    def _extract_error_info(self, error_element):
        """Helper function to extract structured information from an error element."""
        info = {
            "category": "UNKNOWN",
            "subcategory": "UNKNOWN",
            "message": "No message available",
            "code": -1,
            "source": "UNKNOWN",
        }
        try:
            if error_element.hasElement(CATEGORY): info["category"] = error_element.getElementAsString(CATEGORY)
            if error_element.hasElement(SUBCATEGORY): info["subcategory"] = error_element.getElementAsString(SUBCATEGORY)
            if error_element.hasElement(MESSAGE): info["message"] = error_element.getElementAsString(MESSAGE)
            if error_element.hasElement(CODE): info["code"] = error_element.getElementAsInteger(CODE)
            if error_element.hasElement(SOURCE): info["source"] = error_element.getElementAsString(SOURCE)
            # Optionally include the raw element for deeper debugging
            # info["raw_element"] = str(error_element)
        except Exception as e:
            self.logger.error(f"Error extracting details from error element: {e}. Element: {error_element}")
            info["message"] = f"Error parsing error element: {e}"
        return info

    def _parse_security_data(self, sec_data_element, cid_value):
        """Parses a 'securityData' element (for RefData/HistData responses)."""
        result = {"security": "UNKNOWN", "data": {}, "errors": []}
        try:
            if sec_data_element.hasElement(SECURITY_NAME):
                result["security"] = sec_data_element.getElementAsString(SECURITY_NAME)
            sec_name = result["security"] # For logging

            # Check for security-level errors
            if sec_data_element.hasElement(SECURITY_ERROR):
                error_element = sec_data_element.getElement(SECURITY_ERROR)
                error_info = self._extract_error_info(error_element)
                log_msg = f"CID {cid_value}, Security '{sec_name}': SecurityError - Category: {error_info['category']}, Message: {error_info['message']}"
                self.logger.warning(log_msg)
                result["errors"].append({"type": "SecurityError", "details": error_info})
                # Mark data as potentially incomplete or invalid if security error occurs
                result["data"] = {"__security_error__": True} # Indicate error in data part


            # Extract field data (structure differs for HistData vs RefData)
            if sec_data_element.hasElement(FIELD_DATA):
                field_data = sec_data_element.getElement(FIELD_DATA)

                if field_data.isArray(): # HistoricalDataResponse contains an array of daily/periodic data
                    result["data"] = [] # Initialize as list for historical data
                    for daily_data_element in field_data.values():
                        day_result = {}
                        for field_element in daily_data_element.elements():
                            field_name = str(field_element.name())
                            day_result[field_name] = self._get_element_value(field_element, cid_value, sec_name, field_name)
                        # Ensure 'date' field is present if expected (often named 'date')
                        if 'date' not in day_result and daily_data_element.hasElement('date'):
                             day_result['date'] = self._get_element_value(daily_data_element.getElement('date'), cid_value, sec_name, 'date')
                        result["data"].append(day_result)
                else: # ReferenceDataResponse contains a flat structure of fields
                    result["data"] = {} # Initialize as dict for reference data
                    for field_element in field_data.elements():
                        field_name = str(field_element.name())
                        result["data"][field_name] = self._get_element_value(field_element, cid_value, sec_name, field_name)

            # Check for field-level exceptions
            if sec_data_element.hasElement(FIELD_EXCEPTIONS):
                field_exceptions_array = sec_data_element.getElement(FIELD_EXCEPTIONS)
                for exception_element in field_exceptions_array.values():
                    field_id = exception_element.getElementAsString(FIELD_ID)
                    error_info = self._extract_error_info(exception_element.getElement(ERROR_INFO))
                    log_msg = f"CID {cid_value}, Security '{sec_name}', Field '{field_id}': FieldError - Category: {error_info['category']}, Message: {error_info['message']}"
                    self.logger.warning(log_msg)
                    result["errors"].append({"type": "FieldError", "field": field_id, "details": error_info})
                    # Optionally mark the specific field in data as having an error
                    if isinstance(result["data"], dict):
                         result["data"][field_id] = f"__field_error__: {error_info['message']}"

        except Exception as e:
             self.logger.exception(f"CID {cid_value}: Failed to parse securityData element for '{result.get('security', 'UNKNOWN')}': {e}")
             result["errors"].append({"type": "ParsingError", "details": {"message": f"Failed to parse securityData: {e}"}})

        return result

    def _get_element_value(self, element, cid_value, sec_name, field_name):
        """Helper to safely get a value from a blpapi Element, handling types."""
        try:
            dtype = element.datatype()
            if dtype == blpapi.DataType.FLOAT64 or dtype == blpapi.DataType.FLOAT32:
                return element.getValueAsFloat()
            elif dtype == blpapi.DataType.INT64 or dtype == blpapi.DataType.INT32:
                return element.getValueAsInteger()
            elif dtype == blpapi.DataType.DATE:
                dt_val = element.getValueAsDatetime()
                # Return as date object if time is midnight, else keep as datetime
                if dt_val.hour == 0 and dt_val.minute == 0 and dt_val.second == 0 and dt_val.microsecond == 0:
                    return dt_val.date()
                return dt_val
            elif dtype == blpapi.DataType.DATETIME:
                 # Returns a datetime.datetime object
                 return element.getValueAsDatetime()
            elif dtype == blpapi.DataType.BOOL:
                 return element.getValueAsBool()
            elif dtype == blpapi.DataType.STRING:
                 return element.getValueAsString()
            elif element.isArray():
                 # Handle arrays (like in BDS results)
                 array_values = []
                 for item_element in element.values():
                      # Check if array items are complex types (elements themselves)
                      if item_element.numElements() > 0 and item_element.datatype() == blpapi.DataType.SEQUENCE:
                           item_data = {}
                           for sub_element in item_element.elements():
                                sub_name = str(sub_element.name())
                                # Recursively call or handle nested types simply here
                                item_data[sub_name] = self._get_element_value(sub_element, cid_value, sec_name, f"{field_name}.{sub_name}")
                           array_values.append(item_data)
                      else:
                           # Simple value in array
                           array_values.append(self._get_element_value(item_element, cid_value, sec_name, field_name))
                 return array_values
            else:
                # Default or unknown type, get as string
                return element.getValueAsString()
        except Exception as e:
            self.logger.warning(f"CID {cid_value}, Sec '{sec_name}', Field '{field_name}': Error parsing element value (Type: {element.datatype()}): {e}. Returning string representation.")
            try:
                 # Fallback to string representation
                 return element.getValueAsString()
            except Exception as e_str:
                 self.logger.error(f"CID {cid_value}, Sec '{sec_name}', Field '{field_name}': Could not get string fallback value: {e_str}")
                 return "[PARSING_ERROR]"

    def _parse_bar_tick_data(self, bar_tick_element, cid_value):
        """Parses a 'barTickData' element (for IntradayBar responses)."""
        data = {}
        try:
            # Use helper to get values, assuming fields exist
            data['time'] = self._get_element_value(bar_tick_element.getElement(TIME), cid_value, "IntradayBar", "time") if bar_tick_element.hasElement(TIME) else None
            data['open'] = self._get_element_value(bar_tick_element.getElement(OPEN), cid_value, "IntradayBar", "open") if bar_tick_element.hasElement(OPEN) else None
            data['high'] = self._get_element_value(bar_tick_element.getElement(HIGH), cid_value, "IntradayBar", "high") if bar_tick_element.hasElement(HIGH) else None
            data['low'] = self._get_element_value(bar_tick_element.getElement(LOW), cid_value, "IntradayBar", "low") if bar_tick_element.hasElement(LOW) else None
            data['close'] = self._get_element_value(bar_tick_element.getElement(CLOSE), cid_value, "IntradayBar", "close") if bar_tick_element.hasElement(CLOSE) else None
            data['volume'] = self._get_element_value(bar_tick_element.getElement(VOLUME), cid_value, "IntradayBar", "volume") if bar_tick_element.hasElement(VOLUME) else None
            data['numEvents'] = self._get_element_value(bar_tick_element.getElement(NUM_EVENTS), cid_value, "IntradayBar", "numEvents") if bar_tick_element.hasElement(NUM_EVENTS) else None
            # Add other potentially useful fields if needed
        except blpapi.NotFoundException as e:
             self.logger.warning(f"CID {cid_value}: Optional element not found while parsing Intraday Bar data: {e}. Raw: {bar_tick_element}")
        except Exception as e:
            self.logger.error(f"CID {cid_value}: Error parsing Intraday Bar data: {e}. Raw: {bar_tick_element}")
            return None # Indicate parsing failure
        return data

    def _parse_tick_data(self, tick_element, cid_value):
        """Parses a 'tickData' element (nested within TICK_DATA for IntradayTick responses)."""
        data = {}
        try:
            data['time'] = self._get_element_value(tick_element.getElement(TIME), cid_value, "IntradayTick", "time") if tick_element.hasElement(TIME) else None
            data['type'] = self._get_element_value(tick_element.getElement(TYPE), cid_value, "IntradayTick", "type") if tick_element.hasElement(TYPE) else None # e.g., "TRADE", "BID", "ASK"
            data['value'] = self._get_element_value(tick_element.getElement(VALUE), cid_value, "IntradayTick", "value") if tick_element.hasElement(VALUE) else None # Price/Value
            # Add other common tick fields if needed
            if tick_element.hasElement("size"):
                data['size'] = self._get_element_value(tick_element.getElement("size"), cid_value, "IntradayTick", "size")
            if tick_element.hasElement("conditionCodes"):
                 data['conditionCodes'] = self._get_element_value(tick_element.getElement("conditionCodes"), cid_value, "IntradayTick", "conditionCodes")

        except blpapi.NotFoundException as e:
             self.logger.warning(f"CID {cid_value}: Optional element not found while parsing Intraday Tick data: {e}. Raw: {tick_element}")
        except Exception as e:
            self.logger.error(f"CID {cid_value}: Error parsing Intraday Tick data: {e}. Raw: {tick_element}")
            return None # Indicate parsing failure
        return data


    def _process_admin_or_status_event(self, event):
        """Processes administrative and session/service status events."""
        for msg in event:
            msg_type = msg.messageType()
            event_type = event.eventType()
            self.logger.debug(f"Processing Admin/Status Event: Type={event_type}, MsgType={msg_type}")
            self.logger.debug(f"Admin/Status Message Content: {msg}") # Log full message content at debug level

            if msg_type == SESSION_STARTED:
                self.logger.info("SessionStarted event processed.")
                with self.session_status_lock:
                    self.session_status = "started"
                self.session_started_event.set() # Signal successful start
            elif msg_type == SESSION_STARTUP_FAILURE:
                self.logger.error(f"SessionStartupFailure event: {msg}")
                with self.session_status_lock:
                    self.session_status = "failed_to_start"
                self.session_started_event.set() # Signal failure (to unblock wait)
                # No need to set shutdown_event here, start_session will call stop_session
            elif msg_type == SESSION_TERMINATED:
                self.logger.warning(f"SessionTerminated event received: {msg}")
                with self.session_status_lock:
                     self.session_status = "terminated"
                # Also signal the start event just in case start_session was waiting and the session terminated immediately
                # Although ideally, it should have failed or started first.
                self.session_started_event.set()
                self.shutdown_event.set() # Signal event loop to stop
            elif msg_type == SERVICE_OPENED:
                if msg.hasElement("serviceName"):
                    service_name = msg.getElementAsString("serviceName")
                    with self.service_states_lock:
                        self.service_states[service_name] = 'opened'
                    self.logger.info(f"Service opened event processed: {service_name}")
                else:
                    self.logger.warning(f"ServiceOpened event received without serviceName: {msg}")
            elif msg_type == SERVICE_OPEN_FAILURE:
                if msg.hasElement("serviceName"):
                    service_name = msg.getElementAsString("serviceName")
                    with self.service_states_lock:
                        self.service_states[service_name] = 'failed'
                    # Extract reason if available
                    reason_info = "No reason provided"
                    if msg.hasElement(REASON):
                         reason_info = self._extract_error_info(msg.getElement(REASON))
                    self.logger.error(f"Service open failure event processed: {service_name}, Reason: {reason_info}")
                else:
                     self.logger.error(f"ServiceOpenFailure event received without serviceName: {msg}")
            elif msg_type == SLOW_CONSUMER_WARNING:
                self.logger.warning("SlowConsumerWarning event received. Application may not be processing events fast enough.")
            elif msg_type == SLOW_CONSUMER_WARNING_CLEARED:
                 self.logger.info("SlowConsumerWarningCleared event received.")
            elif msg_type == DATA_LOSS:
                service_name = msg.getElementAsString("serviceName") if msg.hasElement("serviceName") else "UNKNOWN"
                num_messages_lost = msg.getElementAsInteger("numMessagesLost") if msg.hasElement("numMessagesLost") else -1
                source = msg.getElementAsString("source") if msg.hasElement("source") else "UNKNOWN"
                self.logger.critical(f"DataLoss event received for service '{service_name}', source '{source}'! Lost approx {num_messages_lost} messages.")
                # Data loss is critical. Application needs to handle this, maybe by resubscribing or requesting snapshots.
            # Handle other important admin/status messages as needed
            # e.g., PermissionRequest, ResolutionSuccess/Failure, etc.
            elif msg_type == PERMISSION_REQUEST:
                 self.logger.warning(f"PermissionRequest event received: {msg}. Entitlements might be missing.")
                 # Application might need to respond to this if using specific entitlement modes

            else:
                self.logger.debug(f"Ignoring admin/status message type: {msg_type}")


    def send_request(self, request, service_name,
                     timeout=None, max_retries=None, retry_delay=None):
        """
        Sends a request, waits for completion or timeout/error, and returns the result.
        Includes retry logic. Uses default values from settings if parameters are None.

        :param request: The blpapi.Request object to send.
        :param service_name: The name of the service (e.g., "//blp/refdata").
        :param timeout: Request timeout in seconds. Defaults to setting['timeout'].
        :param max_retries: Maximum number of retries (0 means one attempt). Defaults to setting['max_retries'].
        :param retry_delay: Initial delay between retries in seconds. Defaults to setting['retry_delay'].
        :return: A dictionary containing the request status, data, and errors.
                 Example: {"status": "complete"|"error", "data": [...], "errors": [...]}
        """
        if not self.session:
            self.logger.error("Session not started. Cannot send request.")
            return {"status": "error", "data": [], "errors": [{"type": "SessionError", "details": {"message": "Session not started."}}]}

        # Use default settings if parameters are not provided
        timeout = timeout if timeout is not None else self.default_timeout
        max_retries = max_retries if max_retries is not None else self.default_max_retries
        retry_delay = retry_delay if retry_delay is not None else self.default_retry_delay

        # Ensure the required service is open (or attempt to open it)
        if not self._open_service(service_name):
            err_msg = f"Failed to open required service: {service_name}"
            self.logger.error(err_msg)
            return {"status": "error", "data": [], "errors": [{"type": "ServiceError", "details": {"message": err_msg}}]}

        # Get the service object (should exist after _open_service succeeds)
        try:
            service = self.session.getService(service_name)
            if not service:
                 # This should ideally not happen if _open_service returned True, but check defensively
                 raise ValueError(f"Could not get service object for {service_name} even after successful open check.")
        except Exception as e:
             err_msg = f"Failed to get service object for {service_name}: {e}"
             self.logger.exception(err_msg) # Log stack trace
             return {"status": "error", "data": [], "errors": [{"type": "ServiceError", "details": {"message": err_msg}}]}


        cid = self._get_next_correlation_id()
        cid_value = cid.value()
        last_exception = None
        current_retry_delay = retry_delay # Initial delay

        for attempt in range(max_retries + 1):
            self.logger.info(f"Sending request CID: {cid_value}, Service: {service_name}, Attempt {attempt + 1}/{max_retries + 1}")
            # Log request details at debug level if needed (can be verbose)
            # self.logger.debug(f"Request details CID {cid_value}: {request}")

            # Initialize response state for this attempt (within lock)
            with self.response_data_lock:
                self.response_data[cid_value] = {"status": "pending", "data": [], "errors": []}

            # Send the request via the session
            try:
                # Pass the identity object if authorization was performed
                self.session.sendRequest(request=request, correlationId=cid, identity=self.identity)
            except Exception as e:
                self.logger.exception(f"Failed to send request (CID: {cid_value}, Attempt {attempt + 1}): {e}")
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Retrying send request in {current_retry_delay:.1f} seconds...")
                    time.sleep(current_retry_delay)
                    current_retry_delay *= 2 # Exponential backoff
                    continue # Go to next attempt
                else:
                    # Max retries reached on send failure
                    err_msg = f"Failed to send request after {max_retries + 1} attempts: {e}"
                    # Ensure final state is error and pop the result
                    with self.response_data_lock:
                        # Safely update and pop
                        final_result = self.response_data.pop(cid_value, {"status": "error", "data": [], "errors": []}) # Default if already popped
                        final_result["status"] = "error"
                        final_result["errors"].append({"type": "SendRequestError", "details": {"message": err_msg}})
                    return final_result

            # --- Wait for completion ---
            start_time = time.time()
            wait_outcome = "pending" # Possible outcomes: "complete", "error", "timeout"
            while True:
                # Check current status under lock
                with self.response_data_lock:
                    # Use .get() to handle cases where data might be removed externally (though unlikely here)
                    response_state = self.response_data.get(cid_value)
                    if response_state:
                         current_status = response_state.get("status", "unknown")
                    else:
                         # Response data disappeared unexpectedly!
                         current_status = "unknown"
                         self.logger.error(f"CID {cid_value}: Response data dictionary entry disappeared while waiting!")

                # --- Check status ---
                if current_status == "complete":
                    self.logger.info(f"CID {cid_value}: Request completed (Attempt {attempt + 1}).")
                    wait_outcome = "complete"
                    break
                elif current_status == "error":
                    self.logger.warning(f"CID {cid_value}: Request processing ended with error status (Attempt {attempt + 1}).")
                    wait_outcome = "error"
                    break
                elif current_status == "unknown":
                    self.logger.error(f"CID {cid_value}: Response data missing or status unknown while waiting.")
                    wait_outcome = "error" # Treat as error
                    last_exception = RuntimeError("Response data missing during wait")
                    break
                elif current_status not in ["pending", "partial"]:
                     self.logger.error(f"CID {cid_value}: Unexpected status '{current_status}' found while waiting.")
                     wait_outcome = "error" # Treat unexpected status as error
                     last_exception = RuntimeError(f"Unexpected status '{current_status}'")
                     # Force state to error under lock
                     with self.response_data_lock:
                          if cid_value in self.response_data:
                               self.response_data[cid_value]["status"] = "error"
                               self.response_data[cid_value]["errors"].append({"type": "InternalError", "details": {"message": f"Unexpected status '{current_status}'"}})
                     break

                # --- Check timeout ---
                if time.time() - start_time > timeout:
                    self.logger.warning(f"Request timed out (CID: {cid_value}, Attempt {attempt + 1}) after {timeout} seconds.")
                    wait_outcome = "timeout"
                    last_exception = TimeoutError(f"Request timed out after {timeout}s")
                    # Mark status as error on timeout under lock
                    with self.response_data_lock:
                         if cid_value in self.response_data:
                              # Only update if still pending/partial
                              if self.response_data[cid_value].get("status") in ["pending", "partial"]:
                                   self.response_data[cid_value]["status"] = "error"
                                   self.response_data[cid_value]["errors"].append({"type": "Timeout", "details": {"message": f"Request timed out after {timeout}s"}})
                    break

                # Brief sleep to yield execution and allow event thread to process
                time.sleep(0.05) # Adjust sleep time as needed

            # --- Process wait outcome ---
            # Get the final result data (pop it from the dict under lock)
            with self.response_data_lock:
                 result = self.response_data.pop(cid_value, None) # Pop safely

            if not result:
                  # Should not happen if wait logic is correct, but handle defensively
                  err_msg = f"Internal Error: Response data for CID {cid_value} was None after waiting (Outcome: {wait_outcome})."
                  self.logger.error(err_msg)
                  last_exception = last_exception or RuntimeError(err_msg) # Keep original exception if timeout occurred
                  if attempt < max_retries:
                       self.logger.warning(f"Retrying due to missing response data in {current_retry_delay:.1f} seconds...")
                       time.sleep(current_retry_delay)
                       current_retry_delay *= 2
                       continue # Go to next attempt
                  else:
                       # Max retries reached after data loss
                       return {"status": "error", "data": [], "errors": [{"type": "InternalError", "details": {"message": err_msg + f" after {max_retries + 1} attempts."}}]}


            # If completed successfully (even if errors occurred within the response)
            if wait_outcome == "complete":
                 self.logger.info(f"CID {cid_value}: Request completed successfully (Attempt {attempt + 1}). Final status: {result.get('status')}")
                 return result # Return the result

            # If timed out or ended in error state, decide whether to retry
            elif wait_outcome == "timeout" or wait_outcome == "error":
                should_retry = False
                if wait_outcome == "timeout":
                    # Timeouts are generally retryable
                    should_retry = True
                    self.logger.warning(f"CID {cid_value}: Request timed out, considering retry.")
                else: # wait_outcome == "error"
                    # Check the errors logged in the result to see if any are retryable
                    for error in result.get("errors", []):
                        details = error.get("details", {})
                        category = details.get("category", "UNKNOWN").upper()
                        subcategory = details.get("subcategory", "UNKNOWN").upper()
                        # Define retryable conditions (needs refinement based on real errors)
                        is_retryable_category = category in RETRYABLE_CATEGORIES or "TIMEOUT" in category or "CONNECTION" in category or "SERVER" in category
                        is_non_retryable_error = category in NON_RETRYABLE_CATEGORIES or subcategory in NON_RETRYABLE_CATEGORIES or "INVALID" in category or "BAD" in category

                        if is_retryable_category and not is_non_retryable_error:
                            should_retry = True
                            last_exception = last_exception or RuntimeError(f"Retryable error detected: Cat={category}, SubCat={subcategory}, Msg={details.get('message','N/A')}")
                            self.logger.warning(f"CID {cid_value}: Detected potentially recoverable error, will retry. Details: {details}")
                            break # Found a retryable error, no need to check others

                # Perform retry if conditions met
                if should_retry and attempt < max_retries:
                    self.logger.info(f"Retrying request CID {cid_value} in {current_retry_delay:.1f} seconds...")
                    time.sleep(current_retry_delay)
                    current_retry_delay *= 2 # Exponential backoff
                    continue # Go to the next attempt in the loop
                elif should_retry and attempt == max_retries:
                    # Max retries reached after a retryable error/timeout
                    self.logger.error(f"Max retries ({max_retries + 1}) reached for CID {cid_value} after encountering retryable error/timeout.")
                    result["errors"].append({"type": "MaxRetriesExceeded", "details": {"message": f"Failed after {max_retries + 1} attempts. Last error indication: {last_exception}"}})
                    result["status"] = "error" # Ensure final status is error
                    return result
                else:
                    # Unrecoverable error or no retry needed
                    self.logger.info(f"CID {cid_value}: Request finished with unrecoverable error or completed with non-retryable errors. Status: {result.get('status')}")
                    return result # Return the final result

            # Should not reach here if wait_outcome is handled correctly
            else:
                 self.logger.error(f"CID {cid_value}: Reached unexpected point after wait loop (Outcome: {wait_outcome}).")
                 result["status"] = "error"
                 result["errors"].append({"type": "InternalError", "details": {"message": f"Unexpected state after wait loop: {wait_outcome}"}})
                 return result


        # Should not be reachable if loop logic is correct
        self.logger.error(f"CID {cid_value}: Exited request loop unexpectedly after {max_retries + 1} attempts.")
        return {"status": "error", "data": [], "errors": [{"type": "Unknown", "details": {"message": f"Exited request loop unexpectedly. Last exception: {last_exception}"}}]}


    # --- Excel-like High-Level Methods ---

    def bdp(self, securities, fields, overrides=None,
            timeout=None, max_retries=None, retry_delay=None):
        """
        Gets static data using ReferenceDataRequest (like Excel BDP).

        :param securities: List/tuple of security identifiers (e.g., ["IBM US Equity", "MSFT US Equity"]) or a single string.
        :param fields: List/tuple of field mnemonics (e.g., ["PX_LAST", "BID", "ASK"]) or a single string.
        :param overrides: Dictionary of overrides (e.g., {"VWAP_START_TIME": "9:30", "VWAP_END_TIME": "16:00"}).
        :param timeout: Specific timeout for this request.
        :param max_retries: Specific max retries for this request.
        :param retry_delay: Specific initial retry delay for this request.
        :return: Dictionary with status, data, and errors.
                 'data' is a list of dicts, one per security: [{"security": "...", "data": {"FIELD": value, ...}, "errors": [...]}, ...]
        """
        self.logger.info(f"Received BDP request for securities: {securities}, fields: {fields}")
        service_name = "//blp/refdata"
        try:
            # Ensure service object is available (will be opened if needed by send_request)
            if not self.session: raise ConnectionError("Session not started.")
            service = self.session.getService(service_name) # Get service obj pre-emptively for request creation
            if not service: raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("ReferenceDataRequest")

            # Standardize inputs to lists
            if isinstance(securities, str): securities = [securities]
            if isinstance(fields, str): fields = [fields]

            # Append securities and fields to the request
            for sec in securities:
                request.append("securities", sec)
            for fld in fields:
                request.append("fields", fld)

            # Add overrides if provided
            if overrides:
                override_element = request.getElement("overrides")
                for key, value in overrides.items():
                    ovrd = override_element.appendElement()
                    ovrd.setElement("fieldId", key)
                    # Ensure value is a string for simplicity, though blpapi might handle types
                    ovrd.setElement("value", str(value))

            # Send the request using the core send_request method
            return self.send_request(request, service_name, timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating BDP request: {e}")
            # Return a structured error if request creation fails
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


        def bdh(self, securities, fields, start_date=None, end_date=None, date=None,
            overrides=None, options=None, timeout=None, max_retries=None, retry_delay=None):
        """
        Gets historical time series data using HistoricalDataRequest (like Excel BDH).

        You must provide EITHER the 'date' argument for a single day's data,
        OR both 'start_date' and 'end_date' for a date range.

        :param securities: List/tuple of security identifiers or a single string.
        :param fields: List/tuple of field mnemonics or a single string.
        :param start_date: Start date ("YYYYMMDD" string or datetime.date/datetime object). Required if 'date' is not provided.
        :param end_date: End date ("YYYYMMDD" string or datetime.date/datetime object). Required if 'date' is not provided.
        :param date: A single specific date ("YYYYMMDD" string or datetime.date/datetime object) for which data is requested. If provided, 'start_date' and 'end_date' must be None.
        :param overrides: Dictionary of overrides.
        :param options: Dictionary of other request options (e.g., {"periodicitySelection": "DAILY", "currency": "USD"}).
        :param timeout: Specific timeout for this request.
        :param max_retries: Specific max retries for this request.
        :param retry_delay: Specific initial retry delay for this request.
        :return: Dictionary with status, data, and errors.
                 'data' is a list of dicts, one per security: [{"security": "...", "data": [{"date": ..., "FIELD": value, ...}, ...], "errors": [...]}, ...]
        """
        # --- Input Validation for Dates ---
        req_start_date_str = None
        req_end_date_str = None

        if date is not None:
            if start_date is not None or end_date is not None:
                raise ValueError("Cannot provide 'date' along with 'start_date' or 'end_date'. Use 'date' for a single day or 'start_date'/'end_date' for a range.")
            self.logger.info(f"Received BDH request for single date: {date}")
            # Format the single date
            if isinstance(date, (datetime, date)):
                req_start_date_str = date.strftime("%Y%m%d")
            else:
                req_start_date_str = str(date) # Assume YYYYMMDD string
            req_end_date_str = req_start_date_str # Use the same date for start and end
        elif start_date is not None and end_date is not None:
            self.logger.info(f"Received BDH request for date range: {start_date} to {end_date}")
            # Format the date range
            if isinstance(start_date, (datetime, date)):
                req_start_date_str = start_date.strftime("%Y%m%d")
            else:
                req_start_date_str = str(start_date)
            if isinstance(end_date, (datetime, date)):
                req_end_date_str = end_date.strftime("%Y%m%d")
            else:
                req_end_date_str = str(end_date)
        else:
            raise ValueError("You must provide either the 'date' argument or both 'start_date' and 'end_date'.")
        # --- End Input Validation ---

        self.logger.info(f"Processing BDH request for {securities}, fields: {fields}, effective period: {req_start_date_str} to {req_end_date_str}")
        service_name = "//blp/refdata"
        try:
            if not self.session: raise ConnectionError("Session not started.")
            service = self.session.getService(service_name)
            if not service: raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("HistoricalDataRequest")

            # Standardize securities/fields inputs
            if isinstance(securities, str): securities = [securities]
            if isinstance(fields, str): fields = [fields]

            for sec in securities:
                request.append("securities", sec)
            for fld in fields:
                request.append("fields", fld)

            # Set the determined start and end dates
            request.set("startDate", req_start_date_str)
            request.set("endDate", req_end_date_str)

            # Set other options if provided
            if options:
                for key, value in options.items():
                    try:
                        if request.hasElement(key):
                            request.set(key, value)
                        else:
                            self.logger.warning(f"BDH: Option '{key}' not found in HistoricalDataRequest schema. Ignoring.")
                    except Exception as e_opt:
                        self.logger.warning(f"BDH: Failed to set option '{key}' to '{value}'. Error: {e_opt}")

            # Add overrides
            if overrides:
                override_element = request.getElement("overrides")
                for key, value in overrides.items():
                    ovrd = override_element.appendElement()
                    ovrd.setElement("fieldId", key)
                    ovrd.setElement("value", str(value))

            # Historical requests can take longer, consider a longer default timeout
            hist_timeout = timeout if timeout is not None else self.default_timeout * 2
            return self.send_request(request, service_name, hist_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating/sending BDH request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}

    def bds(self, securities, field, overrides=None, options=None,
             timeout=None, max_retries=None, retry_delay=None):
        """
        Gets bulk data using ReferenceDataRequest (like Excel BDS).
        Intended for fields that return an array of data.

        :param securities: List/tuple of security identifiers or a single string.
        :param field: The bulk field mnemonic (string, single field only).
        :param overrides: Dictionary of overrides.
        :param options: Dictionary of other ReferenceDataRequest options (rarely needed for BDS).
        :param timeout: Specific timeout for this request.
        :param max_retries: Specific max retries for this request.
        :param retry_delay: Specific initial retry delay for this request.
        :return: Dictionary with status, data, and errors.
                 'data' is a list of dicts, one per security: [{"security": "...", "data": {"FIELD": [item1, item2, ...]}, "errors": [...]}, ...]
                 Items in the array can be simple values or nested dictionaries.
        """
        self.logger.info(f"Received BDS request for {securities}, field: {field}")
        service_name = "//blp/refdata"
        try:
            if not self.session: raise ConnectionError("Session not started.")
            service = self.session.getService(service_name)
            if not service: raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("ReferenceDataRequest")

            if isinstance(securities, str): securities = [securities]
            if not isinstance(field, str):
                 raise ValueError("BDS function requires a single field name (string).")

            for sec in securities:
                request.append("securities", sec)
            request.append("fields", field) # Only one field for BDS

            # Set other options if provided (though less common for BDS)
            if options:
                 for key, value in options.items():
                     try:
                         if request.hasElement(key):
                             request.set(key, value)
                         else:
                              self.logger.warning(f"BDS: Option '{key}' not found in ReferenceDataRequest schema. Ignoring.")
                     except Exception as e_opt:
                         self.logger.warning(f"BDS: Failed to set option '{key}' to '{value}'. Error: {e_opt}")

            # Add overrides
            if overrides:
                override_element = request.getElement("overrides")
                for key, value in overrides.items():
                    ovrd = override_element.appendElement()
                    ovrd.setElement("fieldId", key)
                    ovrd.setElement("value", str(value))

            # BDS can also return large amounts of data, suggest longer timeout
            bds_timeout = timeout if timeout is not None else self.default_timeout * 2
            return self.send_request(request, service_name, bds_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating BDS request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


    def get_intraday_bar(self, security, event_type, start_dt, end_dt, interval,
                         options=None, timeout=None, max_retries=None, retry_delay=None):
        """
        Gets intraday bar data (minute bars, etc.) using IntradayBarRequest.

        :param security: Security identifier (string, single security only for this request type).
        :param event_type: Event type string ("TRADE", "BID", "ASK", "BID_BEST", "ASK_BEST", etc.).
        :param start_dt: Start datetime (datetime object, timezone-aware recommended).
        :param end_dt: End datetime (datetime object, timezone-aware recommended).
        :param interval: Bar interval in minutes (integer).
        :param options: Dictionary of other request options (e.g., {"gapFillInitialBar": True}).
        :param timeout: Specific timeout for this request.
        :param max_retries: Specific max retries for this request.
        :param retry_delay: Specific initial retry delay for this request.
        :return: Dictionary with status, data, and errors.
                 'data' is a list of bar data dicts: [{"time": ..., "open": ..., "high": ..., ...}, ...]
                 Errors related to the request are in the main 'errors' list.
        """
        self.logger.info(f"Received IntradayBar request for {security}, Event: {event_type}, Interval: {interval}, Period: {start_dt} to {end_dt}")
        service_name = "//blp/apidata" # Intraday data typically uses this service
        try:
            if not self.session: raise ConnectionError("Session not started.")
            service = self.session.getService(service_name)
            if not service: raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("IntradayBarRequest")

            # Set mandatory parameters
            request.set("security", security)
            request.set("eventType", event_type)
            request.set("interval", interval) # In minutes

            # Convert Python datetime to blpapi.Datetime objects
            # Using timezone-aware datetimes is highly recommended to avoid ambiguity
            try:
                blp_start_dt = blpapi.Datetime.from_datetime(start_dt)
                blp_end_dt = blpapi.Datetime.from_datetime(end_dt)
            except Exception as dt_err:
                 raise ValueError(f"Invalid start or end datetime provided: {dt_err}") from dt_err

            request.set("startDateTime", blp_start_dt)
            request.set("endDateTime", blp_end_dt)

            # Set optional parameters
            if options:
                for key, value in options.items():
                    try:
                        if request.hasElement(key):
                            # Need to handle type conversions carefully for options
                            # Example: Booleans might need request.set(key, bool(value))
                            request.set(key, value)
                        else:
                             self.logger.warning(f"IntradayBar: Option '{key}' not found in request schema. Ignoring.")
                    except Exception as e_opt:
                        self.logger.warning(f"IntradayBar: Failed to set option '{key}' to '{value}'. Error: {e_opt}")

            # Intraday requests can be very large, use a significantly longer timeout
            intra_timeout = timeout if timeout is not None else self.default_timeout * 4 # Example: 4x default
            return self.send_request(request, service_name, intra_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating IntradayBar request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


    def get_intraday_tick(self, security, event_types, start_dt, end_dt,
                          options=None, timeout=None, max_retries=None, retry_delay=None):
        """
        Gets intraday tick-by-tick data using IntradayTickRequest.

        :param security: Security identifier (string, single security only).
        :param event_types: List/tuple of event type strings (e.g., ["TRADE", "BID", "ASK"]) or a single string.
        :param start_dt: Start datetime (datetime object, timezone-aware recommended).
        :param end_dt: End datetime (datetime object, timezone-aware recommended).
        :param options: Dictionary of other request options (e.g., {"includeConditionCodes": True}).
        :param timeout: Specific timeout for this request.
        :param max_retries: Specific max retries for this request.
        :param retry_delay: Specific initial retry delay for this request.
        :return: Dictionary with status, data, and errors.
                 'data' is a list of tick data dicts: [{"time": ..., "type": ..., "value": ..., ...}, ...]
        """
        self.logger.info(f"Received IntradayTick request for {security}, Events: {event_types}, Period: {start_dt} to {end_dt}")
        service_name = "//blp/apidata"
        try:
            if not self.session: raise ConnectionError("Session not started.")
            service = self.session.getService(service_name)
            if not service: raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("IntradayTickRequest")

            request.set("security", security)

            # Add event types (must be a list/sequence)
            if isinstance(event_types, str): event_types = [event_types]
            for et in event_types:
                request.append("eventTypes", et)

            # Convert Python datetime to blpapi.Datetime
            try:
                blp_start_dt = blpapi.Datetime.from_datetime(start_dt)
                blp_end_dt = blpapi.Datetime.from_datetime(end_dt)
            except Exception as dt_err:
                 raise ValueError(f"Invalid start or end datetime provided: {dt_err}") from dt_err

            request.set("startDateTime", blp_start_dt)
            request.set("endDateTime", blp_end_dt)

            # Set optional parameters
            if options:
                 for key, value in options.items():
                     try:
                         if request.hasElement(key):
                             # Handle type conversions, e.g., boolean options
                             if isinstance(value, bool):
                                 request.set(key, value)
                             else:
                                  # Assume string or let blpapi handle conversion
                                 request.set(key, value)
                         else:
                              self.logger.warning(f"IntradayTick: Option '{key}' not found in request schema. Ignoring.")
                     except Exception as e_opt:
                         self.logger.warning(f"IntradayTick: Failed to set option '{key}' to '{value}'. Error: {e_opt}")

            # Tick data can be extremely large, use a very long timeout
            intra_timeout = timeout if timeout is not None else self.default_timeout * 6 # Example: 6x default
            return self.send_request(request, service_name, intra_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating IntradayTick request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}

    # --- Context Manager Implementation ---
    def __enter__(self):
        """Context Manager: Start the session."""
        if not self.start_session(): # Assumes start_session can handle auth if needed via config/defaults
            # Clean up if start fails partially? stop_session handles None session.
            self.stop_session()
            raise RuntimeError("Failed to start Bloomberg session.")
        return self # Return the wrapper instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context Manager: Stop the session."""
        self.stop_session()
        # Do not suppress exceptions, return False or None (default)


# --- asyncio Integration Helper ---
async def run_blp_request_async(blp_wrapper: BlpApiWrapper, request_func, *args, **kwargs):
    """
    Helper function to run a synchronous BlpApiWrapper method asynchronously
    from an asyncio event loop using asyncio.to_thread (Python 3.9+).

    :param blp_wrapper: The BlpApiWrapper instance.
    :param request_func: The synchronous method to call (e.g., blp_wrapper.bdp).
    :param args: Positional arguments for the request function.
    :param kwargs: Keyword arguments for the request function.
    :return: The result dictionary from the request function.
    """
    # Use asyncio.to_thread to run the blocking function in a separate thread
    # managed by the asyncio event loop's default executor.
    # This prevents the blocking call from stalling the event loop.
    result = await asyncio.to_thread(request_func, *args, **kwargs)

    # For Python < 3.9, use loop.run_in_executor:
    # loop = asyncio.get_running_loop()
    # result = await loop.run_in_executor(None, request_func, *args, **kwargs) # None uses default ThreadPoolExecutor

    return result

# --- Usage Examples ---
async def main_async_example():
    """Example of using the wrapper asynchronously with asyncio."""
    blp = BlpApiWrapper(config_file='config.ini') # Create wrapper instance

    # Start session synchronously before running async tasks
    # Or handle session start/stop within the async function if preferred,
    # but ensure it's done before requests.
    if not blp.start_session():
        print("Failed to start session for async example.")
        return

    try:
        print("\n--- Sending requests asynchronously using asyncio ---")

        # Create coroutine for a BDP request
        bdp_task = run_blp_request_async(
            blp,
            blp.bdp, # The method to call
            securities=["IBM US Equity", "MSFT US Equity", "INVALID SEC"],
            fields=["PX_LAST", "SECURITY_NAME_REALTIME", "RT_EXCH_TRADE_VOL"]
        )

        # Create coroutine for a BDH request
        from datetime import date, timedelta
        start_date_h = date.today() - timedelta(days=7)
        end_date_h = date.today() - timedelta(days=1)
        bdh_task = run_blp_request_async(
            blp,
            blp.bdh, # The method to call
            securities="AAPL US Equity",
            fields=["PX_LAST", "VOLUME"],
            start_date=start_date_h, # Pass date objects directly
            end_date=end_date_h,
            options={"periodicitySelection": "DAILY"}
        )

        # --- Simulate another concurrent I/O task (e.g., database query) ---
        # Replace this with your actual async database call if using an asyncio-compatible driver
        async def dummy_db_query(duration=1.5):
            print("Starting dummy DB query...")
            await asyncio.sleep(duration) # Simulate I/O wait
            print("Finished dummy DB query.")
            return {"db_data": "some result from database"}

        db_task = dummy_db_query()

        # --- Run all tasks concurrently and wait for results ---
        print("Gathering async tasks...")
        # results will be a list containing the return values of the tasks in order
        results = await asyncio.gather(bdp_task, bdh_task, db_task, return_exceptions=True)
        print("Async tasks gathered.")

        # Process results (check for exceptions if return_exceptions=True)
        bdp_result_async = results[0]
        bdh_result_async = results[1]
        db_result = results[2]

        print("\n--- Async BDP Result ---")
        if isinstance(bdp_result_async, Exception):
             print(f"BDP Task failed: {bdp_result_async}")
        else:
             # Use default=str to handle datetime objects in json.dumps
             print(json.dumps(bdp_result_async, indent=2, default=str))

        print("\n--- Async BDH Result ---")
        if isinstance(bdh_result_async, Exception):
             print(f"BDH Task failed: {bdh_result_async}")
        else:
             print(json.dumps(bdh_result_async, indent=2, default=str))

        print(f"\n--- Other Async Task (DB) Result ---")
        if isinstance(db_result, Exception):
             print(f"DB Task failed: {db_result}")
        else:
             print(db_result)

    except Exception as e:
        print(f"An error occurred in async example: {e}")
        blp.logger.exception("Error during async execution")
    finally:
        blp.stop_session() # Ensure session is stopped


if __name__ == '__main__':
    from datetime import date, timedelta, datetime

    # --- Synchronous Usage Example ---
    print("--- Synchronous Example ---")
    try:
        # Use context manager for automatic session start/stop
        with BlpApiWrapper(config_file='config.ini') as blp: # Creates instance and starts session

            # BDP Example Call
            print("\n--- BDP Call (Sync) ---")
            bdp_result_sync = blp.bdp(
                securities=["NVDA US Equity", "AMD US Equity", "NONEXISTENT"],
                fields=["PX_LAST", "CHG_PCT_1D", "BEST_EPS"],
                overrides={"BEST_FPERIOD_OVERRIDE": "1BF"} # Example: Next fiscal period estimate
            )
            print("BDP Sync Result:")
            # Use default=str for json.dumps to handle potential date/datetime objects
            print(json.dumps(bdp_result_sync, indent=2, default=str))

            # BDH Example Call
            print("\n--- BDH Call (Sync) ---")
            today = date.today()
            start_dt_h = today - timedelta(days=30)
            bdh_result_sync = blp.bdh(
                securities="GOOGL UW Equity", # Single string also works
                fields=["PX_LAST", "TURNOVER"],
                start_date=start_dt_h, # Pass date object
                end_date=today, # Pass date object
                options={"periodicitySelection": "WEEKLY", "nonTradingDayFillOption": "ACTIVE_DAYS_ONLY"}
            )
            print("BDH Sync Result:")
            print(json.dumps(bdh_result_sync, indent=2, default=str))

            # BDS Example Call
            print("\n--- BDS Call (Sync) ---")
            bds_result_sync = blp.bds(
                securities="INDU Index",
                field="INDX_MWEIGHT", # Index constituent weights (bulk field)
                overrides={"END_DATE_OVERRIDE": "20240131"} # Example override
            )
            print("BDS Sync Result:")
            print(json.dumps(bds_result_sync, indent=2, default=str))

            # Intraday Bar Example Call (Uncomment to run)
            # print("\n--- Intraday Bar Call (Sync) ---")
            # now = datetime.now() # Use timezone-aware datetime if possible!
            # start_dt_bar = now - timedelta(minutes=60)
            # end_dt_bar = now
            # # Ensure start/end times are within allowed intraday history limits (e.g., ~140 days)
            # try:
            #     intraday_bar_result = blp.get_intraday_bar(
            #         security="MSFT US Equity",
            #         event_type="TRADE",
            #         start_dt=start_dt_bar,
            #         end_dt=end_dt_bar,
            #         interval=5 # 5-minute bars
            #     )
            #     print("Intraday Bar Sync Result:")
            #     print(json.dumps(intraday_bar_result, indent=2, default=str))
            # except ValueError as ve:
            #      print(f"Could not run Intraday Bar example: {ve}")


        # Session is automatically stopped here by __exit__

    except RuntimeError as e:
        # Catch session start/stop errors from context manager
        print(f"Runtime Error (likely session related): {e}")
    except ConnectionError as e:
        print(f"Connection Error: {e}")
    except Exception as e:
        # Catch other potential errors during synchronous execution
        print(f"An unexpected error occurred in sync example: {e}")
        # Log exception if logger was configured
        logger = logging.getLogger('blpapi_wrapper')
        if logger.hasHandlers():
             logger.exception("Unhandled error in main sync execution")


    # --- Asynchronous (asyncio) Usage Example ---
    print("\n\n--- Asynchronous Example using asyncio ---")
    # Run the async main function using asyncio.run()
    try:
        # asyncio.run() creates a new event loop and runs the coroutine until completion
        asyncio.run(main_async_example())
    except Exception as e:
        print(f"Error running asyncio example: {e}")
        logger = logging.getLogger('blpapi_wrapper')
        if logger.hasHandlers():
             logger.exception("Unhandled error in main async execution")
