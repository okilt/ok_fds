import blpapi # type: ignore
import asyncio
import logging
from datetime import datetime, date, time, timezone # Use datetime.timezone for UTC
import pytz # For localizing datetimes if user provides non-UTC
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError, before_sleep_log
from typing import Any, List, Dict, Optional, Callable, Tuple, Union

# Import custom exceptions (adjust path if needed)
from bloomberg_exceptions import (
    BloombergError, BloombergConnectionError, BloombergRequestError, BloombergTimeoutError,
    BloombergSecurityError, BloombergFieldError, BloombergDataError,
    BloombergPartialDataError, BloombergLimitError
)
# Import mock pool manager for example (replace with your actual manager path)
# from connection_pool_manager import MockConnectionPoolManager


# --- Constants for BLPAPI ---
# Service names
REF_DATA_SVC_URI = "//blp/refdata"
MKT_BAR_SVC_URI = "//blp/mktbar"
API_AUTH_SVC_URI = "//blp/apiauth" # For server-side applications needing authorization

# Request names
REFERENCE_DATA_REQUEST = blpapi.Name("ReferenceDataRequest")
HISTORICAL_DATA_REQUEST = blpapi.Name("HistoricalDataRequest")
INTRADAY_BAR_REQUEST = blpapi.Name("IntradayBarRequest")
INTRADAY_TICK_REQUEST = blpapi.Name("IntradayTickRequest")
# AUTHORIZATION_REQUEST = blpapi.Name("AuthorizationRequest") # If using server-side auth

# Event types / Message types (blpapi.Name for message types)
SESSION_STARTED = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE = blpapi.Name("ServiceOpenFailure")
AUTHORIZATION_SUCCESS = blpapi.Name("AuthorizationSuccess") # If using server-side auth
AUTHORIZATION_FAILURE = blpapi.Name("AuthorizationFailure") # If using server-side auth

PARTIAL_RESPONSE = blpapi.Name("PartialResponse") # EventType
RESPONSE = blpapi.Name("Response") # EventType
REQUEST_FAILURE = blpapi.Name("RequestFailure") # MessageType for bad request
SESSION_TERMINATED = blpapi.Name("SessionTerminated") # MessageType

# Element names (commonly used)
SECURITY_DATA = blpapi.Name("securityData")
SECURITY_NAME = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")
SECURITY_ERROR = blpapi.Name("securityError")
RESPONSE_ERROR = blpapi.Name("responseError") # Top-level error in a message
CATEGORY = blpapi.Name("category")
MESSAGE = blpapi.Name("message")
BAR_DATA = blpapi.Name("barData")
BAR_TICK_DATA = blpapi.Name("barTickData") # Array of bars
TICK_DATA_ARRAY = blpapi.Name("tickData") # Outer array of ticks

# Configure logging
logger = logging.getLogger(__name__)

# Constants for lightweight network test
_APIFLDS_SVC_URI = "//blp/apiflds"
_FIELD_SEARCH_REQUEST_NAME = blpapi.Name("FieldSearchRequest") # Renamed to avoid conflict
_FIELD_RESPONSE_NAME = blpapi.Name("FieldResponse") # For FieldSearch response

# Session Test Modes
SessionTestMode = Literal["TIMESTAMP", "LOCAL_STATE", "NETWORK_LIGHT"]

# Retry configuration
RETRYABLE_EXCEPTIONS = (
    BloombergConnectionError,
    BloombergTimeoutError,
    # Retry if a partial data error resulted in NO actual data
    lambda e: isinstance(e, BloombergPartialDataError) and not e.partial_data,
    # Retry on generic limit errors (could be temporary)
    BloombergLimitError,
    # Specific BLPAPI internal errors that might be transient
    lambda e: isinstance(e, BloombergError) and any(kw in str(e).upper() for kw in ["TIMEOUT", "CONNECTION", "SERVICEUNAVAILABLE"])
)

# Helper to run async function from sync context
def _run_async_from_sync(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # No running event loop
        loop = None

    if loop and loop.is_running():
        # This is a common issue. Simplest is to advise using async methods directly.
        # More complex solutions like nest_asyncio or running in a new thread exist.
        # For this wrapper, we assume sync methods are for non-async native environments.
        raise RuntimeError(
            "Sync wrapper called from a running asyncio event loop. "
            "Please use the 'async_*' version of the method in an async context."
        )
    return asyncio.run(coro)

# Helper to iterate over elements of a blpapi.Element if it's a sequence
def _element_generator(element: blpapi.Element):
    """Generator to yield sub-elements from a blpapi.Element of SEQUENCE type."""
    for i in range(element.numElements()):
        yield element.getElementAt(i)


class BloombergAPIWrapper:
    def __init__(self,
                 connection_pool_manager: Any, # Your ConnectionPoolManager instance
                 host: str = 'localhost',
                 port: int = 8194,
                 request_timeout_ms: int = 30000, # Timeout for waiting for blpapi.Session.nextEvent()
                 session_startup_timeout_ms: int = 15000, # Timeout for session to start
                 service_open_timeout_ms: int = 10000, # Timeout for a service to open
                 max_retries: int = 3,
                 retry_wait_base_secs: int = 2,
                 # For server-side auth. If None, client-side auth (Desktop API) is assumed.
                 auth_options: Optional[str] = None # E.g., "APPLICATION:APP_NAME" or "USER_AND_APPLICATION:APP_NAME"
                 session_activity_test_threshold_sec: float = 0.5,
                 session_test_mode: SessionTestMode = "LOCAL_STATE", # Default to local_state
                 # Timeout specifically for the network part of _test_blp_session
                 session_network_test_timeout_ms: int = 300): # Aggressive timeout for test
                 ):
        self.connection_pool_manager = connection_pool_manager
        self.connection_params = {'host': host, 'port': port}
        self.request_timeout_ms = request_timeout_ms
        self.session_startup_timeout_ms = session_startup_timeout_ms
        self.service_open_timeout_ms = service_open_timeout_ms
        self.auth_options = auth_options # For server-side auth
        self.session_activity_test_threshold_sec = session_activity_test_threshold_sec
        self.session_test_mode = session_test_mode
        self.session_network_test_timeout_ms = session_network_test_timeout_ms

        # Define retry decorator for instance methods
        self.retry_decorator = retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=retry_wait_base_secs, min=1, max=30),
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING), # Log before retrying
        )

    async def _wait_for_session_event(self, session: blpapi.Session,
                                      success_msg_type: blpapi.Name,
                                      failure_msg_type: blpapi.Name,
                                      context_description: str,
                                      timeout_ms: int,
                                      target_service_name: Optional[str] = None): # For service events
        loop = asyncio.get_event_loop()
        end_time = loop.time() + timeout_ms / 1000.0

        while True:
            now = loop.time()
            if now >= end_time:
                raise BloombergTimeoutError(f"Timeout waiting for {context_description} ({success_msg_type.string()} or {failure_msg_type.string()}).")

            remaining_timeout_for_next_event = int((end_time - now) * 1000)
            if remaining_timeout_for_next_event <= 0: remaining_timeout_for_next_event = 1

            event = await asyncio.to_thread(session.nextEvent, remaining_timeout_for_next_event)

            if event.eventType() == blpapi.Event.TIMEOUT:
                continue

            for msg in event:
                is_relevant_message = False
                # For service-related events, ensure it's for the target service
                if target_service_name:
                    if msg.hasElement("serviceName") and msg.getElementAsString("serviceName") == target_service_name:
                        is_relevant_message = True
                else: # For session-level events (SessionStarted, Auth events)
                    is_relevant_message = True

                if not is_relevant_message:
                    continue

                if msg.messageType() == success_msg_type:
                    logger.info(f"{context_description}: Event '{success_msg_type.string()}' received.")
                    return
                elif msg.messageType() == failure_msg_type:
                    reason = msg.getElementAsString('reason') if msg.hasElement('reason') else 'Unknown reason'
                    logger.error(f"{context_description}: Event '{failure_msg_type.string()}' received. Reason: {reason}")
                    raise BloombergConnectionError(f"{context_description} failed: {reason}",
                                                   details={"message_type": failure_msg_type.string(), "reason": reason})
                # else:
                    # logger.debug(f"Ignoring message type {msg.messageType()} while waiting for {context_description}")

    async def _authorize_session(self, session: blpapi.Session, identity: blpapi.Identity):
        """Handles server-side authorization if auth_options are provided."""
        if not self.auth_options:
            logger.info("No server-side auth options provided. Assuming client-side (Desktop API) authentication.")
            return # No explicit server-side authorization needed

        logger.info(f"Attempting server-side authorization with options: {self.auth_options}")
        # Open //blp/apiauth service
        if not session.openService(API_AUTH_SVC_URI):
            raise BloombergConnectionError(f"Failed to initiate opening service: {API_AUTH_SVC_URI}")
        await self._wait_for_session_event(
            session, SERVICE_OPENED, SERVICE_OPEN_FAILURE,
            f"Service Opening ({API_AUTH_SVC_URI})", self.service_open_timeout_ms,
            target_service_name=API_AUTH_SVC_URI
        )
        
        auth_service = session.getService(API_AUTH_SVC_URI)
        auth_request = auth_service.createAuthorizationRequest()
        auth_request.set("token", self.auth_options) # Example: "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=my_app_name"
                                                    # Or "AuthenticationType=OS_LOGON_USER" or "USER_AND_APPLICATION" etc.
                                                    # The exact format depends on your BBG setup (SAPI/BPIPE).

        # Generate a new correlation ID for the authorization request
        auth_cid = blpapi.CorrelationId(f"auth_{int(asyncio.get_event_loop().time()*1000)}")
        session.sendAuthorizationRequest(auth_request, identity, auth_cid)
        
        logger.info(f"Authorization request sent with CID: {auth_cid}. Waiting for response.")
        # Wait for AuthorizationSuccess or AuthorizationFailure
        # These are MESSAGE types, not event types. They arrive in Admin or Session_Status events.
        # The _wait_for_session_event needs to be adapted or a new one created to check CID.
        # For now, let's assume the wait_for_session_event can handle this by message type if CID is not checked by it.
        # For simplicity, this example assumes Authorization events come without specific CID check in _wait_for_session_event.
        # A more robust solution would track CID for auth events.
        await self._wait_for_session_event(
            session, AUTHORIZATION_SUCCESS, AUTHORIZATION_FAILURE,
            "Server-Side Authorization", self.session_startup_timeout_ms # Reuse session timeout for auth
        )
        logger.info("Server-side authorization successful.")


    async def _create_blp_session(self, host: str, port: int) -> blpapi.Session:
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(host)
        session_options.setServerPort(port)
        # Set other options like auto-restart, client credentials for SAPI, etc.
        # if self.auth_options and "uuid" in self.auth_options.lower(): # Example for SAPI needing UUID
        #    session_options.setSessionIdentityOptions(authOptions, cid_for_auth) # More complex SAPI setup

        logger.info(f"Creating Bloomberg session to {host}:{port}")
        session = blpapi.Session(session_options)

        if not session.start(): # Non-blocking, initiates connection
            raise BloombergConnectionError("Failed to initiate Bloomberg session start (session.start() returned false).")

        logger.info("Session start initiated. Waiting for SessionStarted or SessionStartupFailure.")
        try:
            await self._wait_for_session_event(
                session, SESSION_STARTED, SESSION_STARTUP_FAILURE,
                "Session Startup", self.session_startup_timeout_ms
            )
            logger.info("Bloomberg session started successfully.")

            # Handle server-side authorization if configured
            if self.auth_options:
                identity = session.createIdentity() # Create an identity object for this session
                await self._authorize_session(session, identity)
                # Store identity with session or make it accessible for requests that need it
                session.blpapi_identity = identity # Attach for later use
            else:
                session.blpapi_identity = None # No specific identity for Desktop API in this context

        except BloombergError as e:
            logger.error(f"Error during session startup or authorization: {e}")
            await asyncio.to_thread(session.stop) # Ensure session is stopped if startup fails
            raise

        async def aclose_session():
            logger.info(f"Asynchronously stopping Bloomberg session: {session}")
            if session:
                await asyncio.to_thread(session.stop)
        session.aclose = aclose_session # Attach for the pool manager

        return session

    async def _test_blp_session_network_light(self, session: blpapi.Session) -> bool:
        """
        Performs a lightweight network test (FieldSearchRequest).
        This is meant to be called only if other faster checks fail.
        Has its own aggressive timeout.
        """
        cid = None
        test_passed = False
        # Use self.session_network_test_timeout_ms
        loop = asyncio.get_event_loop()
        overall_test_end_time = loop.time() + self.session_network_test_timeout_ms / 1000.0

        try:
            logger.debug(f"Session test (NETWORK_LIGHT): Attempting for session {session}")

            # Phase 1: Open //blp/apiflds service if not already open
            # Use a portion of the overall test timeout for this.
            service_open_timeout_s = self.session_network_test_timeout_ms / 2000.0 # e.g., 150ms if total is 300ms
            
            # Check if service is already open to avoid unnecessary blocking openService call
            # This is a quick local check.
            is_apiflds_open = _APIFLDS_SVC_URI in session.getOpenServices()

            if not is_apiflds_open:
                logger.debug(f"Session test (NETWORK_LIGHT): {_APIFLDS_SVC_URI} not open, attempting to open.")
                open_service_task = asyncio.to_thread(session.openService, _APIFLDS_SVC_URI)
                try:
                    await asyncio.wait_for(open_service_task, timeout=service_open_timeout_s)
                    logger.debug(f"Session test (NETWORK_LIGHT): {_APIFLDS_SVC_URI} opened successfully.")
                except asyncio.TimeoutError:
                    logger.warning(f"Session test (NETWORK_LIGHT): Timeout opening {_APIFLDS_SVC_URI} for session {session}.")
                    return False # Fail test if service open times out
                except blpapi.Exception as e:
                    logger.warning(f"Session test (NETWORK_LIGHT): Failed to open {_APIFLDS_SVC_URI} for session {session}: {e}")
                    return False # Fail test if service open fails
            else:
                logger.debug(f"Session test (NETWORK_LIGHT): {_APIFLDS_SVC_URI} already open.")


            service = session.getService(_APIFLDS_SVC_URI) # Should exist now
            if not service: # Should not happen if openService succeeded
                logger.error(f"Session test (NETWORK_LIGHT): Service {_APIFLDS_SVC_URI} not available after open.")
                return False

            request = service.createRequest(_FIELD_SEARCH_REQUEST_NAME.string())
            request.set("searchSpec", "PX_LAST") # Common field
            request.set("includeFieldInfo", False) # Minimal data

            cid = blpapi.CorrelationId(f"test_net_{int(monotime.monotonic()*1000000)}")
            identity = getattr(session, 'blpapi_identity', None)

            logger.debug(f"Session test (NETWORK_LIGHT): Sending FieldSearchRequest CID {cid.value()}")
            
            # Timeout for the sendRequest operation itself. Should be quick.
            # Use a small fraction of the remaining time or a fixed small timeout.
            # Let's use a fixed small timeout for the send operation itself for simplicity.
            SEND_REQUEST_OP_TIMEOUT_S = 0.05 # 50ms for the send operation itself            
            # time_left_for_send_s = max(0.001, (overall_test_end_time - loop.time())) # Ensure positive
            
            send_task = asyncio.to_thread(session.sendRequest, request,
                                          identity=identity if identity else None,
                                          correlationId=cid)
            try:
                await asyncio.wait_for(send_task, timeout=SEND_REQUEST_OP_TIMEOUT_S)
            except asyncio.TimeoutError:
                logger.warning(f"Session test (NETWORK_LIGHT): Timeout during sendRequest operation for CID {cid.value()}. FAIL.")
                # If sendRequest itself times out, the session is likely unresponsive.
                return False 
            # If sendRequest raised blpapi.Exception, it will be caught by the outer try-except
            
            # Phase 2: Wait for a response
            while loop.time() < overall_test_end_time:
                remaining_event_timeout_ms = max(1, int((overall_test_end_time - loop.time()) * 1000))
                event = await asyncio.to_thread(session.nextEvent, remaining_event_timeout_ms) # Use remaining time
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    continue

                for msg in event:
                    if cid in msg.correlationIds():
                        if msg.messageType() == RESPONSE or msg.messageType() == _FIELD_RESPONSE_NAME:
                            logger.debug(f"Session test (NETWORK_LIGHT): Received response for CID {cid.value()}. PASS.")
                            test_passed = True
                            return True # Test success
                        elif msg.messageType() == REQUEST_FAILURE:
                            err_info = msg.getElement(RESPONSE_ERROR)
                            logger.warning(f"Session test (NETWORK_LIGHT): RequestFailure CID {cid.value()}: {err_info}. FAIL.")
                            return False # Test fail
                if test_passed: break # Should be caught by return True above

            if not test_passed:
                logger.warning(f"Session test (NETWORK_LIGHT): Timeout waiting for response to CID {cid.value()}. FAIL.")
            return False # Timeout for response

        except asyncio.TimeoutError: # Catches wait_for timeouts
            logger.warning(f"Session test (NETWORK_LIGHT): Operation timed out for session {session}.")
            return False
        except blpapi.Exception as e:
            logger.warning(f"Session test (NETWORK_LIGHT): BLPAPI exception for session {session}: {e}")
            return False
        except Exception as e:
            logger.error(f"Session test (NETWORK_LIGHT): Unexpected error for session {session}: {e}", exc_info=True)
            return False
        finally:
            if cid and not test_passed: # If request was sent but didn't complete successfully
                try:
                    # Cancel needs a list of CIDs
                    await asyncio.to_thread(session.cancel, [cid])
                    logger.debug(f"Session test (NETWORK_LIGHT): Cancelled test CID {cid.value()}")
                except Exception as cancel_e:
                    logger.warning(f"Session test (NETWORK_LIGHT): Error cancelling test CID {cid.value()}: {cancel_e}")


    async def _test_blp_session(self, session: blpapi.Session) -> bool:
        """
        Tests the Bloomberg session based on the configured session_test_mode.
        """
        # Tier 1: Timestamp check (always performed, super fast)
        last_success_time = getattr(session, '_last_successful_request_time', 0)
        if (monotime.monotonic() - last_success_time) < self.session_activity_test_threshold_sec:
            logger.debug(f"Session test (mode: {self.session_test_mode}): Timestamp check PASSED (recently active).")
            return True
        
        logger.debug(f"Session test (mode: {self.session_test_mode}): Timestamp check FAILED (idle > {self.session_activity_test_threshold_sec}s).")

        if self.session_test_mode == "TIMESTAMP":
            # If only timestamp mode, and it failed, then the test fails.
            return False

        # Tier 2: Local state check (getOpenServices)
        try:
            open_services = session.getOpenServices() # Fast, local, synchronous call
            if not open_services:
                # If no services are open locally, and it's not a brand new session (which timestamp would've caught),
                # this might indicate an issue or an uninitialized session from the pool.
                logger.info(f"Session test (mode: {self.session_test_mode}): LOCAL_STATE check FAILED (no open services locally).")
                if self.session_test_mode == "LOCAL_STATE":
                    return False
                # For NETWORK_LIGHT, we might still proceed to network test even if no services are locally "open",
                # as the network test will try to open //blp/apiflds.
                # However, if getOpenServices itself fails (e.g. InvalidStateException), we stop.
            else:
                 logger.debug(f"Session test (mode: {self.session_test_mode}): LOCAL_STATE check PASSED (has open services: {open_services}).")
                 if self.session_test_mode == "LOCAL_STATE":
                    return True # Passed local state, and that's the configured mode

        except blpapi.InvalidStateException as e:
            logger.warning(f"Session test (mode: {self.session_test_mode}): LOCAL_STATE check FAILED (InvalidStateException: {e}).")
            return False # Session is in a bad state, definitely fail.
        except Exception as e: # Catch other potential blpapi errors from getOpenServices
            logger.warning(f"Session test (mode: {self.session_test_mode}): LOCAL_STATE check FAILED (Exception: {e}).")
            return False

        # Tier 3: Lightweight Network Test (if configured and previous checks didn't fully pass/fail for the mode)
        if self.session_test_mode == "NETWORK_LIGHT":
            logger.debug(f"Session test (mode: {self.session_test_mode}): Proceeding to NETWORK_LIGHT check.")
            network_test_ok = await self._test_blp_session_network_light(session)
            if network_test_ok:
                # If network test passes, update activity timestamp as it was successfully used
                setattr(session, '_last_successful_request_time', monotime.monotonic())
                logger.info(f"Session test (mode: {self.session_test_mode}): NETWORK_LIGHT check PASSED.")
            else:
                logger.info(f"Session test (mode: {self.session_test_mode}): NETWORK_LIGHT check FAILED.")
            return network_test_ok
        
        # Fallback, should not be reached if logic is correct for TIMESTAMP and LOCAL_STATE modes
        # If mode was LOCAL_STATE and open_services was not empty, it would have returned True.
        # If mode was LOCAL_STATE and open_services was empty, it would have returned False.
        logger.error(f"Session test: Reached unexpected fallback in _test_blp_session for mode {self.session_test_mode}.")
        return False # Default to false if logic error

    def _parse_element_value(self, element: blpapi.Element) -> Any:
        dtype = element.datatype()
        if dtype == blpapi.DataType.STRING: return element.getValueAsString()
        if dtype in (blpapi.DataType.FLOAT32, blpapi.DataType.FLOAT64): return element.getValueAsFloat()
        if dtype in (blpapi.DataType.INT32, blpapi.DataType.INT64): return element.getValueAsInteger()
        if dtype == blpapi.DataType.BOOL: return element.getValueAsBool()
        if dtype == blpapi.DataType.DATE:
            dt = element.getValueAsDatetime()
            return date(dt.year, dt.month, dt.day)
        if dtype == blpapi.DataType.TIME:
            dt = element.getValueAsDatetime()
            return time(dt.hour, dt.minute, dt.second, dt.microsecond)
        if dtype == blpapi.DataType.DATETIME:
            dt_val = element.getValueAsDatetime()
            # BLPAPI datetime objects are naive but generally represent UTC for historical/intraday data.
            return datetime(dt_val.year, dt_val.month, dt_val.day,
                            dt_val.hour, dt_val.minute, dt_val.second, dt_val.microsecond,
                            tzinfo=timezone.utc) # Attach UTC timezone
        if element.isArray():
            return [self._parse_element_value(element.getValue(i)) for i in range(element.numValues())]
        if dtype == blpapi.DataType.SEQUENCE and not element.isArray(): # A single complex element
            return {str(sub_element.name()): self._parse_element_value(sub_element)
                    for sub_element in _element_generator(element)}
        try:
            return element.getValueAsString() # Fallback
        except Exception:
            logger.warning(f"Unsupported/unhandled data type {dtype} for element {element.name()}")
            return None

    def _extract_errors(self, msg: blpapi.Message, securities_in_request: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        errors = []
        # Top-level response error
        if msg.hasElement(RESPONSE_ERROR):
            err_info = msg.getElement(RESPONSE_ERROR)
            errors.append({
                "type": "RESPONSE_ERROR", "security": "N/A", # Not specific to one security
                "source": err_info.getElementAsString("source") if err_info.hasElement("source") else "N/A",
                "code": err_info.getElementAsInteger("code") if err_info.hasElement("code") else -1,
                "category": err_info.getElementAsString(CATEGORY) if err_info.hasElement(CATEGORY) else "N/A",
                "message": err_info.getElementAsString(MESSAGE) if err_info.hasElement(MESSAGE) else "N/A",
            })

        # Security-level errors (common in RefData)
        if msg.hasElement(SECURITY_DATA):
            sec_data_array = msg.getElement(SECURITY_DATA)
            for i in range(sec_data_array.numValues()):
                sec_data = sec_data_array.getValueAsElement(i)
                sec_name = sec_data.getElementAsString(SECURITY_NAME)

                if sec_data.hasElement(SECURITY_ERROR):
                    sec_error = sec_data.getElement(SECURITY_ERROR)
                    errors.append({
                        "type": "SECURITY_ERROR", "security": sec_name,
                        "source": sec_error.getElementAsString("source") if sec_error.hasElement("source") else "N/A",
                        "code": sec_error.getElementAsInteger("code") if sec_error.hasElement("code") else -1,
                        "category": sec_error.getElementAsString(CATEGORY) if sec_error.hasElement(CATEGORY) else "N/A",
                        "message": sec_error.getElementAsString(MESSAGE) if sec_error.hasElement(MESSAGE) else "N/A",
                    })

                if sec_data.hasElement(FIELD_EXCEPTIONS):
                    field_exc_array = sec_data.getElement(FIELD_EXCEPTIONS)
                    for j in range(field_exc_array.numValues()):
                        field_exc = field_exc_array.getValueAsElement(j)
                        error_info = field_exc.getElement(ERROR_INFO)
                        errors.append({
                            "type": "FIELD_ERROR", "security": sec_name,
                            "field": field_exc.getElementAsString(FIELD_ID),
                            "source": error_info.getElementAsString("source") if error_info.hasElement("source") else "N/A",
                            "code": error_info.getElementAsInteger("code") if error_info.hasElement("code") else -1,
                            "category": error_info.getElementAsString(CATEGORY) if error_info.hasElement(CATEGORY) else "N/A",
                            "message": error_info.getElementAsString(MESSAGE) if error_info.hasElement(MESSAGE) else "N/A",
                        })
        # Handle cases where the entire request might have failed for a known security (e.g. IntradayBarRequest)
        # This is heuristic if `securities_in_request` is provided and is small (e.g., for single-security intraday)
        if not errors and msg.messageType() == REQUEST_FAILURE and securities_in_request and len(securities_in_request) == 1:
            # This might be a failure for the single security in request
            err_info = msg.getElement(RESPONSE_ERROR) # Assuming REQUEST_FAILURE populates RESPONSE_ERROR
            errors.append({
                "type": "REQUEST_FAILURE_AS_SECURITY_ERROR", "security": securities_in_request[0],
                "source": err_info.getElementAsString("source") if err_info.hasElement("source") else "N/A",
                "code": err_info.getElementAsInteger("code") if err_info.hasElement("code") else -1,
                "category": err_info.getElementAsString(CATEGORY) if err_info.hasElement(CATEGORY) else "N/A",
                "message": err_info.getElementAsString(MESSAGE) if err_info.hasElement(MESSAGE) else "Request failed",
            })
        return errors

    async def _async_send_request(self,
                                  service_uri: str,
                                  request_type_name: blpapi.Name,
                                  populate_request_func: Callable[[blpapi.Request], None],
                                  parse_response_func: Callable[[blpapi.Message], List[Dict[str, Any]]],
                                  securities_in_request: Optional[List[str]] = None
                                 ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        session = None
        cid = None
        try:
            session = await self.connection_pool_manager.get_connection(
                create_func=self._create_blp_session,
                test_func=self._test_blp_session,
                **self.connection_params
            )

            # Open service (this is non-blocking, initiates opening)
            if not session.openService(service_uri):
                raise BloombergConnectionError(f"Call to session.openService() for {service_uri} failed to initiate.")
            
            try: # Wait for service to open
                await self._wait_for_session_event(
                    session, SERVICE_OPENED, SERVICE_OPEN_FAILURE,
                    f"Service Opening ({service_uri})", self.service_open_timeout_ms,
                    target_service_name=service_uri
                )
            except BloombergError as e:
                # Specific handling if service opening itself fails.
                # This might be retryable if it's a timeout or connection blip.
                category = "SERVICE_OPEN_FAILURE"
                if isinstance(e, BloombergTimeoutError): category = "SERVICE_OPEN_TIMEOUT"
                raise BloombergConnectionError(
                    f"Failed or timed out opening service {service_uri}: {e}",
                    details={"category": category, "original_error": str(e)}
                ) from e

            service = session.getService(service_uri)
            request = service.createRequest(request_type_name.string()) # Request name is string
            populate_request_func(request)

            identity = getattr(session, 'blpapi_identity', None) # Get identity if SAPI is used
            cid = blpapi.CorrelationId(f"req_{int(asyncio.get_event_loop().time()*1000000)}") # Unique CID

            logger.debug(f"Sending request (CID: {cid.value()}) to {service_uri} for {request_type_name.string()}")
            if identity:
                session.sendRequest(request, identity=identity, correlationId=cid)
            else:
                session.sendRequest(request, correlationId=cid) # Desktop API

            all_data: List[Dict[str, Any]] = []
            all_errors: List[Dict[str, Any]] = []
            is_final_response = False
            
            loop = asyncio.get_event_loop()
            request_end_time = loop.time() + self.request_timeout_ms / 1000.0

            while not is_final_response:
                now = loop.time()
                if now >= request_end_time:
                    # Cancel request on Bloomberg side if possible
                    if cid: session.cancel(cid)
                    raise BloombergTimeoutError(f"Overall request timeout ({self.request_timeout_ms}ms) for CID {cid.value()}")

                # Calculate remaining time for nextEvent; blpapi needs positive integer
                remaining_event_timeout = max(1, int((request_end_time - now) * 1000))
                event = await asyncio.to_thread(session.nextEvent, remaining_event_timeout)

                if event.eventType() == blpapi.Event.TIMEOUT:
                    # nextEvent timed out, main loop will check overall request_end_time
                    logger.debug(f"nextEvent timed out for CID {cid.value()}, continuing to wait for response.")
                    continue
                
                # Handle session/service status events that might occur mid-request
                if event.eventType() == blpapi.Event.SESSION_STATUS:
                    for msg in event:
                        if msg.messageType() == SESSION_TERMINATED:
                            logger.error(f"Session terminated mid-request (CID: {cid.value()}): {msg}")
                            raise BloombergConnectionError(f"Session terminated: {msg.getElementAsString('reason') if msg.hasElement('reason') else 'Unknown'}")
                    continue # Continue waiting for response events

                # Process messages for our correlation ID
                for msg in event:
                    if cid in msg.correlationIds():
                        logger.debug(f"Received message type: {msg.messageType().string()} for CID {cid.value()}")
                        
                        # Extract errors first
                        msg_errors = self._extract_errors(msg, securities_in_request)
                        all_errors.extend(msg_errors)

                        if msg.messageType() == REQUEST_FAILURE:
                            # This is a failure of the request structure itself or a critical error
                            err_info = msg.getElement(RESPONSE_ERROR) # Should be present
                            err_msg_text = err_info.getElementAsString(MESSAGE) if err_info.hasElement(MESSAGE) else "Request failed"
                            category = err_info.getElementAsString(CATEGORY) if err_info.hasElement(CATEGORY) else "N/A"
                            if "LIMIT" in category.upper() or "LIMIT" in err_msg_text.upper():
                                raise BloombergLimitError(f"Request failed due to limit (CID: {cid.value()}): {err_msg_text}", details=msg_errors)
                            raise BloombergRequestError(f"Request failed (CID: {cid.value()}): {err_msg_text}", details=msg_errors)

                        # Parse data from the message
                        try:
                            parsed_data = parse_response_func(msg)
                            all_data.extend(parsed_data)
                        except Exception as parse_exc:
                            logger.error(f"Error parsing response message (CID: {cid.value()}): {parse_exc}", exc_info=True)
                            all_errors.append({"type": "PARSE_ERROR", "message": str(parse_exc), "cid": cid.value()})

                        if event.eventType() == RESPONSE: # Final response event for this request
                            logger.debug(f"Final response event received for CID {cid.value()}")
                            is_final_response = True
                            break # Break from message loop, then from while loop
            
            return all_data, all_errors

        except RetryError as e_retry: # Tenacity specific
            logger.error(f"Request failed after {self.retry_decorator.retry.statistics.get('attempt_number', max_retries)} retries (CID: {cid.value() if cid else 'N/A'}): {e_retry.last_attempt.exception()}")
            raise BloombergError(f"Request failed after max retries.", details=str(e_retry.last_attempt.exception())) from e_retry.last_attempt.exception()
        except BloombergError: # Re-raise our custom errors
            raise
        except blpapi.Exception as e_blp: # Wrap blpapi native exceptions
            logger.error(f"BLPAPI internal error (CID: {cid.value() if cid else 'N/A'}): {e_blp}", exc_info=True)
            raise BloombergError(f"BLPAPI internal error: {e_blp}") from e_blp
        except Exception as e_generic: # Catch any other unexpected errors
            logger.error(f"Unexpected error during request (CID: {cid.value() if cid else 'N/A'}): {e_generic}", exc_info=True)
            raise BloombergError(f"Unexpected error: {e_generic}") from e_generic
        finally:
            if session:
                await self.connection_pool_manager.release_connection(session)

    def _apply_overrides(self, request: blpapi.Request, overrides: Optional[Dict[str, str]] = None):
        if overrides:
            override_element = request.getElement("overrides")
            for key, value in overrides.items():
                ovr = override_element.appendElement()
                ovr.setElement("fieldId", key)
                ovr.setElement("value", str(value)) # Ensure value is string

    def _handle_response_data_and_errors(self, data: List[Dict], errors: List[Dict], requested_securities: List[str], request_type: str):
        """Common logic to raise exceptions based on data and errors."""
        if errors:
            # Check if all requested securities failed with security-level errors
            sec_error_count = sum(1 for e in errors if e['type'] == 'SECURITY_ERROR' and e.get('security') in requested_securities)
            if not data and sec_error_count == len(requested_securities) and requested_securities:
                raise BloombergSecurityError(f"All securities in {request_type} request failed.", details=errors)
            
            # Check for general response errors that indicate a broader issue
            response_errors = [e for e in errors if e['type'] == 'RESPONSE_ERROR']
            if not data and response_errors:
                # Heuristic: if first response error mentions NO_DATA or similar for intraday, raise DataError
                first_resp_err_msg = response_errors[0].get('message','').upper()
                if any(kw in first_resp_err_msg for kw in ["NO DATA", "NO EVENTS", "NO TICKS", "NOT FOUND"]):
                    raise BloombergDataError(f"{request_type} request returned no data due to: {response_errors[0].get('message')}", details=errors)
                # Otherwise, could be a more general request problem
                # raise BloombergRequestError(f"{request_type} failed with response error(s).", details=errors)
            
            # If we have some data OR some errors that aren't full failure for all secs
            if data or (errors and not (not data and sec_error_count == len(requested_securities))):
                 raise BloombergPartialDataError(f"{request_type} request completed with some errors.", data, errors)

        if not data and requested_securities: # No data and no specific errors captured above that raised.
             raise BloombergDataError(f"No data returned for {request_type} request and no specific errors identified.", details=errors)
        
        return data


    # --- BDP: Current Data ---
    def _parse_bdp_response(self, msg: blpapi.Message) -> List[Dict[str, Any]]:
        records = []
        if not msg.hasElement(SECURITY_DATA): return records
        security_data_array = msg.getElement(SECURITY_DATA)
        for i in range(security_data_array.numValues()):
            security_data_item = security_data_array.getValueAsElement(i)
            if security_data_item.hasElement(SECURITY_ERROR): continue # Error already extracted

            sec_name = security_data_item.getElementAsString(SECURITY_NAME)
            field_data_element = security_data_item.getElement(FIELD_DATA)
            record = {"security": sec_name}
            for field_element in _element_generator(field_data_element):
                field_name = str(field_element.name())
                try:
                    record[field_name] = self._parse_element_value(field_element)
                except Exception as e:
                    logger.warning(f"Error parsing BDP field {field_name} for {sec_name}: {e}")
                    record[field_name] = f"PARSE_ERROR: {e}"
            records.append(record)
        return records

    async def async_bdp(self, securities: Union[str, List[str]],
                        fields: Union[str, List[str]],
                        overrides: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        secs = [securities] if isinstance(securities, str) else list(securities)
        flds = [fields] if isinstance(fields, str) else list(fields)

        def populate_request(request: blpapi.Request):
            for sec in secs: request.getElement("securities").appendValue(sec)
            for fld in flds: request.getElement("fields").appendValue(fld)
            self._apply_overrides(request, overrides)
        
        async def request_with_retry():
            data, errors = await self._async_send_request(
                REF_DATA_SVC_URI, REFERENCE_DATA_REQUEST, populate_request, self._parse_bdp_response, secs
            )
            return self._handle_response_data_and_errors(data, errors, secs, "BDP")
        return await self.retry_decorator(request_with_retry)()

    # --- BDH: Historical Data ---
    def _parse_bdh_response(self, msg: blpapi.Message) -> List[Dict[str, Any]]:
        records = []
        if not msg.hasElement(SECURITY_DATA): return records
        
        # BDH typically returns one securityData element per message if the request has multiple securities,
        # or one securityData containing all data if the request is for a single security.
        # The structure is securityData -> fieldData (array of dates) -> elements for each field.
        security_data_item = msg.getElement(SECURITY_DATA) # This is usually not an array itself in BDH responses per message.
                                                           # It's the container for *one* security's historical data.

        if security_data_item.hasElement(SECURITY_ERROR): return records # Error for this security already captured
        
        sec_name = security_data_item.getElementAsString(SECURITY_NAME)
        field_data_array = security_data_item.getElement(FIELD_DATA) # This is an array of daily/periodical entries
        
        for i in range(field_data_array.numValues()):
            period_data = field_data_array.getValueAsElement(i)
            record = {"security": sec_name}
            if period_data.hasElement("date"): # Date is fundamental
                record["date"] = self._parse_element_value(period_data.getElement("date"))
            
            for field_element in _element_generator(period_data):
                if str(field_element.name()) == "date": continue # Already processed
                field_name = str(field_element.name())
                try:
                    record[field_name] = self._parse_element_value(field_element)
                except Exception as e:
                    logger.warning(f"Error parsing BDH field {field_name} for {sec_name} on {record.get('date')}: {e}")
                    record[field_name] = f"PARSE_ERROR: {e}"
            records.append(record)
        return records

    async def async_bdh(self, securities: Union[str, List[str]], fields: Union[str, List[str]],
                        start_date: str, end_date: str, # YYYYMMDD
                        periodicity_adjustment: str = "ACTUAL", periodicity_selection: str = "DAILY",
                        overrides: Optional[Dict[str, str]] = None, currency: Optional[str] = None
                       ) -> List[Dict[str, Any]]:
        secs = [securities] if isinstance(securities, str) else list(securities)
        flds = [fields] if isinstance(fields, str) else list(fields)

        def populate_request(request: blpapi.Request):
            for sec in secs: request.getElement("securities").appendValue(sec)
            for fld in flds: request.getElement("fields").appendValue(fld)
            request.set("startDate", start_date)
            request.set("endDate", end_date)
            request.set("periodicityAdjustment", periodicity_adjustment)
            request.set("periodicitySelection", periodicity_selection)
            if currency: request.set("currency", currency)
            self._apply_overrides(request, overrides)

        async def request_with_retry():
            data, errors = await self._async_send_request(
                REF_DATA_SVC_URI, HISTORICAL_DATA_REQUEST, populate_request, self._parse_bdh_response, secs
            )
            return self._handle_response_data_and_errors(data, errors, secs, "BDH")
        return await self.retry_decorator(request_with_retry)()

    # --- BDS: Bulk Data ---
    async def async_bds(self, securities: Union[str, List[str]], field: str,
                        overrides: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        # BDS is like BDP but typically for a single field that returns structured bulk data.
        # Parsing is similar to BDP; the _parse_element_value handles arrays/sequences.
        secs = [securities] if isinstance(securities, str) else list(securities)

        def populate_request(request: blpapi.Request):
            for sec in secs: request.getElement("securities").appendValue(sec)
            request.getElement("fields").appendValue(field) # Single BDS field
            self._apply_overrides(request, overrides)

        async def request_with_retry():
            data, errors = await self._async_send_request(
                REF_DATA_SVC_URI, REFERENCE_DATA_REQUEST, populate_request, self._parse_bdp_response, secs # Reuse BDP parser
            )
            processed_data = self._handle_response_data_and_errors(data, errors, secs, f"BDS (field: {field})")
            
            # Additional BDS validation: ensure the requested field is present.
            for record in processed_data:
                if field not in record:
                    logger.warning(f"BDS field '{field}' missing in record for security '{record.get('security')}'. "
                                   f"This might be due to no data for that field/security combination or a parse error.")
                    # Depending on strictness, this could be an error.
                    # For now, allow it, as it might be legitimate "no data for this field".
            return processed_data
        return await self.retry_decorator(request_with_retry)()


    # --- Intraday Bar Data ---
    def _parse_intraday_bar_response(self, msg: blpapi.Message) -> List[Dict[str, Any]]:
        records = []
        if not msg.hasElement(BAR_DATA): return records
        bar_data_element = msg.getElement(BAR_DATA)
        if not bar_data_element.hasElement(BAR_TICK_DATA): return records

        bar_tick_data_array = bar_data_element.getElement(BAR_TICK_DATA) # Array of bars
        for i in range(bar_tick_data_array.numValues()):
            bar = bar_tick_data_array.getValueAsElement(i) # A single bar (sequence)
            record = {}
            for field_element in _element_generator(bar):
                field_name = str(field_element.name())
                try:
                    record[field_name] = self._parse_element_value(field_element)
                except Exception as e:
                    logger.warning(f"Error parsing Intraday Bar field {field_name}: {e}")
                    record[field_name] = f"PARSE_ERROR: {e}"
            records.append(record)
        return records

    async def async_get_intraday_bars(self, security: str, event_type: str, # "TRADE", "BID", "ASK", etc.
                                      start_datetime: datetime, # Naive or aware datetime in user's local TZ
                                      end_datetime: datetime,   # Naive or aware datetime in user's local TZ
                                      user_timezone: str,       # E.g., "America/New_York"
                                      interval_minutes: int,
                                      gap_fill_initial_bar: bool = False,
                                      adjustment_normal: bool = True, adjustment_abnormal: bool = True,
                                      adjustment_split: bool = True, adjustment_follow_prd: bool = True
                                     ) -> List[Dict[str, Any]]:
        try:
            tz = pytz.timezone(user_timezone)
        except pytz.UnknownTimeZoneError:
            raise ValueError(f"Unknown timezone string: {user_timezone}")

        # Convert user's local datetimes to UTC for BLPAPI
        start_utc = tz.localize(start_datetime) if start_datetime.tzinfo is None else start_datetime.astimezone(tz)
        start_utc = start_utc.astimezone(pytz.utc)
        end_utc = tz.localize(end_datetime) if end_datetime.tzinfo is None else end_datetime.astimezone(tz)
        end_utc = end_utc.astimezone(pytz.utc)

        blp_start_dt = blpapi.Datetime(start_utc.year, start_utc.month, start_utc.day,
                                       start_utc.hour, start_utc.minute, start_utc.second)
        blp_end_dt = blpapi.Datetime(end_utc.year, end_utc.month, end_utc.day,
                                     end_utc.hour, end_utc.minute, end_utc.second)

        def populate_request(request: blpapi.Request):
            request.set("security", security)
            request.set("eventType", event_type.upper())
            request.set("interval", interval_minutes)
            request.set("startDateTime", blp_start_dt)
            request.set("endDateTime", blp_end_dt)
            if gap_fill_initial_bar: request.set("gapFillInitialBar", True)
            request.set("adjustmentNormal", adjustment_normal)
            request.set("adjustmentAbnormal", adjustment_abnormal)
            request.set("adjustmentSplit", adjustment_split)
            request.set("adjustmentFollowDPDF", adjustment_follow_prd)

        async def request_with_retry():
            # Intraday requests are typically for a single security. Errors are often in responseError.
            data, errors = await self._async_send_request(
                MKT_BAR_SVC_URI, INTRADAY_BAR_REQUEST, populate_request, self._parse_intraday_bar_response, [security]
            )
            return self._handle_response_data_and_errors(data, errors, [security], f"IntradayBar ({security})")
        return await self.retry_decorator(request_with_retry)()

    # --- Intraday Tick Data ---
    def _parse_intraday_tick_response(self, msg: blpapi.Message) -> List[Dict[str, Any]]:
        records = []
        if not msg.hasElement(TICK_DATA_ARRAY): return records # Outer array element
        
        tick_data_container = msg.getElement(TICK_DATA_ARRAY)
        if not tick_data_container.isArray():
             logger.warning(f"Expected {TICK_DATA_ARRAY.string()} to be an array, but it's not.")
             return records

        for i in range(tick_data_container.numValues()):
            tick_event = tick_data_container.getValueAsElement(i) # A single tick (sequence)
            record = {}
            for field_element in _element_generator(tick_event):
                field_name = str(field_element.name())
                try:
                    record[field_name] = self._parse_element_value(field_element)
                except Exception as e:
                    logger.warning(f"Error parsing Intraday Tick field {field_name}: {e}")
                    record[field_name] = f"PARSE_ERROR: {e}"
            records.append(record)
        return records

    async def async_get_intraday_ticks(self, security: str, event_types: List[str], # E.g., ["TRADE", "BID"]
                                       start_datetime: datetime, # Naive or aware datetime in user's local TZ
                                       end_datetime: datetime,   # Naive or aware datetime in user's local TZ
                                       user_timezone: str,       # E.g., "America/New_York"
                                       include_condition_codes: bool = False,
                                       include_non_plottable_events: bool = False,
                                       # include_exchange_codes: bool = False, # Optional, check schema if needed
                                      ) -> List[Dict[str, Any]]:
        try:
            tz = pytz.timezone(user_timezone)
        except pytz.UnknownTimeZoneError:
            raise ValueError(f"Unknown timezone string: {user_timezone}")

        start_utc = tz.localize(start_datetime) if start_datetime.tzinfo is None else start_datetime.astimezone(tz)
        start_utc = start_utc.astimezone(pytz.utc)
        end_utc = tz.localize(end_datetime) if end_datetime.tzinfo is None else end_datetime.astimezone(tz)
        end_utc = end_utc.astimezone(pytz.utc)

        blp_start_dt = blpapi.Datetime(start_utc.year, start_utc.month, start_utc.day,
                                       start_utc.hour, start_utc.minute, start_utc.second)
        blp_end_dt = blpapi.Datetime(end_utc.year, end_utc.month, end_utc.day,
                                     end_utc.hour, end_utc.minute, end_utc.second)

        def populate_request(request: blpapi.Request):
            request.set("security", security)
            evt_types_elem = request.getElement("eventTypes")
            for etype in event_types: evt_types_elem.appendValue(etype.upper())
            request.set("startDateTime", blp_start_dt)
            request.set("endDateTime", blp_end_dt)
            if include_condition_codes: request.set("includeConditionCodes", True)
            if include_non_plottable_events: request.set("includeNonPlottableEvents", True)
            # if include_exchange_codes: request.set("includeExchangeCodes", True) # If schema supports

        async def request_with_retry():
            data, errors = await self._async_send_request(
                MKT_BAR_SVC_URI, INTRADAY_TICK_REQUEST, populate_request, self._parse_intraday_tick_response, [security]
            )
            return self._handle_response_data_and_errors(data, errors, [security], f"IntradayTick ({security})")
        return await self.retry_decorator(request_with_retry)()

    # --- Synchronous Wrappers ---
    def bdp(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return _run_async_from_sync(self.async_bdp(*args, **kwargs))
    def bdh(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return _run_async_from_sync(self.async_bdh(*args, **kwargs))
    def bds(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return _run_async_from_sync(self.async_bds(*args, **kwargs))
    def get_intraday_bars(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return _run_async_from_sync(self.async_get_intraday_bars(*args, **kwargs))
    def get_intraday_ticks(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return _run_async_from_sync(self.async_get_intraday_ticks(*args, **kwargs))

    async def close(self):
        """Releases resources, primarily by instructing the connection pool manager to clean up."""
        if hasattr(self.connection_pool_manager, 'close_all'):
            logger.info("Closing all connections in the pool manager.")
            await self.connection_pool_manager.close_all()
        else:
            logger.warning("Connection pool manager does not have a 'close_all' method. Cannot explicitly close all connections.")
        logger.info("BloombergAPIWrapper closed.")


# Example Usage (Illustrative - requires Bloomberg environment and blpapi installed)
async def main_example():
    # Configure logging for detailed output
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.DEBUG) # Set wrapper's logger to DEBUG for more verbosity

    # Use the mock connection pool manager for this example
    # In a real application, you'd instantiate your actual pool manager
    from connection_pool_manager import MockConnectionPoolManager
    pool_mgr = MockConnectionPoolManager(max_connections=2)

    # Create the wrapper instance
    # For SAPI/BPIPE (server-side auth), you might need auth_options:
    # E.g., auth_options="AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=your_app_name"
    # (Details depend on your firm's Bloomberg server configuration)
    # For Desktop API, auth_options is typically None.
    bbg = BloombergAPIWrapper(pool_mgr, host='localhost', port=8194, auth_options=None)

    try:
        # === BDP Example ===
        print("\n--- BDP Example ---")
        try:
            bdp_data = await bbg.async_bdp(
                securities=["IBM US Equity", "AAPL US Equity", "INVALID SEC"],
                fields=["PX_LAST", "BID", "ASK", "MARKET_STATUS"],
                overrides={"VWAP_START_TIME": "09:30:00", "VWAP_END_TIME": "16:00:00"}
            )
            print("BDP Data:")
            for record in bdp_data: print(record)
        except BloombergPartialDataError as e:
            print(f"BDP Partial Data Error: {e.args[0]}")
            print("Partial Data:", e.partial_data)
            print("Errors:", e.errors)
        except BloombergError as e:
            print(f"BDP Error: {e}")

        # === BDH Example ===
        print("\n--- BDH Example ---")
        try:
            bdh_data = await bbg.async_bdh(
                securities="MSFT US Equity",
                fields=["PX_LAST", "VOLUME"],
                start_date="20230101",
                end_date="20230110",
                periodicity_selection="DAILY"
            )
            print("BDH Data:")
            for record in bdh_data: print(record)
        except BloombergError as e:
            print(f"BDH Error: {e}")

        # === BDS Example ===
        print("\n--- BDS Example (Index Members) ---")
        try:
            # Example: Get index members for S&P 500. Field for this is often 'INDX_MEMBERS'.
            # This often requires specific overrides like 'END_DATE_OVERRIDE' for a specific date.
            bds_data = await bbg.async_bds(
                securities="SPX Index",
                field="INDX_MEMBERS" # This field returns an array of structs
                # overrides={"END_DATE_OVERRIDE": "20230630"} # Example override, may vary
            )
            print("BDS Data (INDX_MEMBERS for SPX):")
            for record in bds_data:
                print(f"Security: {record.get('security')}")
                if 'INDX_MEMBERS' in record:
                    print(f"  Members count: {len(record['INDX_MEMBERS'])}")
                    # for member in record['INDX_MEMBERS'][:5]: print(f"    {member}") # Print first 5 members
                else:
                    print(f"  INDX_MEMBERS field not found or empty for {record.get('security')}")

        except BloombergError as e:
            print(f"BDS Error: {e}")


        # === Intraday Bar Example ===
        print("\n--- Intraday Bar Example ---")
        user_tz = "America/New_York" # User's local timezone
        start_dt_local = datetime(2023, 7, 10, 9, 30, 0)  # 9:30 AM New York
        end_dt_local = datetime(2023, 7, 10, 10, 0, 0)    # 10:00 AM New York
        try:
            intraday_bars = await bbg.async_get_intraday_bars(
                security="AAPL US Equity",
                event_type="TRADE",
                start_datetime=start_dt_local,
                end_datetime=end_dt_local,
                user_timezone=user_tz,
                interval_minutes=1
            )
            print(f"Intraday Bars for AAPL US Equity (1-minute trades, {start_dt_local} to {end_dt_local} {user_tz}):")
            for bar in intraday_bars[:5]: print(bar) # Print first 5 bars
            if not intraday_bars: print("No intraday bars returned.")
        except BloombergError as e:
            print(f"Intraday Bar Error: {e}")


        # === Intraday Tick Example ===
        print("\n--- Intraday Tick Example ---")
        # For ticks, use a very short time interval for demonstration
        tick_start_local = datetime(2023, 7, 10, 9, 30, 0)
        tick_end_local = datetime(2023, 7, 10, 9, 30, 30) # 30 seconds of ticks
        try:
            intraday_ticks = await bbg.async_get_intraday_ticks(
                security="MSFT US Equity",
                event_types=["TRADE", "BID", "ASK"],
                start_datetime=tick_start_local,
                end_datetime=tick_end_local,
                user_timezone=user_tz,
                include_condition_codes=True
            )
            print(f"Intraday Ticks for MSFT US Equity ({tick_start_local} to {tick_end_local} {user_tz}):")
            for tick in intraday_ticks[:10]: print(tick) # Print first 10 ticks
            if not intraday_ticks: print("No intraday ticks returned.")
        except BloombergError as e:
            print(f"Intraday Tick Error: {e}")

        # === Synchronous call example (if not in an async context already) ===
        # print("\n--- Synchronous BDP Example ---")
        # try:
        #     sync_bdp_data = bbg.bdp("GOOG US Equity", "PX_LAST")
        #     print(sync_bdp_data)
        # except RuntimeError as e:
        #     print(f"Caught expected RuntimeError for sync call from async: {e}")
        # except BloombergError as e:
        #     print(f"Sync BDP Error: {e}")


    finally:
        await bbg.close() # Important to clean up resources

if __name__ == "__main__":
    # To run the example:
    # 1. Ensure you have a Bloomberg connection (Terminal, SAPI, BPIPE).
    # 2. Install blpapi: pip install blpapi
    # 3. Install tenacity: pip install tenacity
    # 4. Install pytz: pip install pytz
    # 5. Save the files: connection_pool_manager.py, bloomberg_exceptions.py, bloomberg_wrapper.py
    # 6. Run: python bloomberg_wrapper.py
    
    # Note: The example uses a mock connection pool. For real use, integrate your actual pool manager.
    # If you get "RuntimeError: Sync wrapper called from a running asyncio event loop",
    # it's because the sync example part is called from within main_example (which is async).
    # To test sync calls properly, call them from a non-async script.
    try:
        asyncio.run(main_example())
    except RuntimeError as e:
        if "cannot be called from a running event loop" in str(e):
             print("Example main finished. Note: If you saw RuntimeErrors for sync calls, "
                  "it's because they were called from an async context for demo. "
                  "Test sync calls from a purely synchronous script.")
        else:
            raise