"""
Flask lifecycle hooks for API request/response logging.  Uses the existing global logger configured in app.py.

Provides before_request and after_request hooks that log API usage in using
Common Log Format, as previously used for API Gateway custom access log format on AWS.
https://en.wikipedia.org/wiki/Common_Log_Format#Combined_Log_Format

Log format:
    $sourceIp $caller $user [$requestTime] "$method $resourcePath $protocol" $status $responseLength $requestId
replacement for AWS API Gateway custom access log format:
    $context.identity.sourceIp $context.identity.caller $context.identity.user [$context.requestTime]
    "$context.httpMethod $context.resourcePath $context.protocol"
    $context.status $context.responseLength $context.requestId

Example log output:
    [2026-03-18 18:52:25] API_USAGE in setup_lifecycle_hooks: Request started: DELETE /flush-cache/12345678901234567890123456789012 from 172.19.0.1 [ID: req-1773859945850-1262]
    [2026-03-18 18:52:25] API_USAGE in setup_lifecycle_hooks: 172.19.0.1 - - [18/Mar/2026:18:52:25 +0000] "DELETE /flush-cache/12345678901234567890123456789012 HTTP/1.1" 200 69 req-1773859945850-1262
"""

import logging
import time
from flask import request, g
from datetime import datetime, timezone

# Use the same logger configuration as app.py
logger = logging.getLogger(__name__)

# For the hooks used to log endpoint usage, set the level to use while
# logging these events, and to be used to return quickly when the
# logger is not enabled for that level.
ENDPOINT_LOG_LEVEL=logging.INFO-1
logging.addLevelName(ENDPOINT_LOG_LEVEL, "API_USAGE")

def setup_flask_lifecycle_hooks(app):
    """
    Register Flask lifecycle hooks for request/response logging.
    
    Sets up before_request and after_request handlers that log all API calls
    using the existing logger configured in app.py.
    
    Args:
        app: Flask application instance
        
    Usage:
        from setup_lifecycle_hooks import setup_flask_lifecycle_hooks
        
        app = Flask(__name__)
        # ... existing logger configuration ...
        setup_flask_lifecycle_hooks(app)
    """
    
    @app.before_request
    def log_endpoint_request():
        """
        Log basic request information at ENDPOINT_LOG_LEVEL level when request starts.
        
        Runs BEFORE any route function executes.
        Captures request start time and generates unique request ID.
        """
        # Bail out on this hook method immediately if the logger statement at
        # the end of the method would not be logged.
        if not logger.isEnabledFor(ENDPOINT_LOG_LEVEL):
            return

        # Store request start time for potential duration calculation
        g.request_start_time = time.time()
        
        # Generate unique request ID for tracking this request
        g.request_id = f"req-{int(time.time() * 1000)}-{hash(request.remote_addr) % 10000}"
        
        logger.log(level=ENDPOINT_LOG_LEVEL
                   , msg=   f"Request started: {request.method} {request.path} "
                            f"from {request.remote_addr} [ID: {g.request_id}]")
    
    @app.after_request
    def log_endpoint_response(response):
        """
        Log complete API usage in AWS API Gateway format at INFO level.
        
        Runs AFTER route function executes (or after error handler if route failed).
        Has access to both request and response data.
        
        Format matches AWS API Gateway custom access logs:
            $sourceIp $caller $user [$requestTime] "$method $resourcePath $protocol" $status $responseLength $requestId
        
        Args:
            response: Flask response object
            
        Returns:
            response: Must return the response unchanged
        """
        # Bail out on this hook method immediately if the logger statement at
        # the end of the method would not be logged.
        if not logger.isEnabledFor(ENDPOINT_LOG_LEVEL):
            return response

        # Extract request details
        source_ip = request.remote_addr or '-'
        
        # Caller - not available without AWS IAM, use '-'
        caller = '-'
        
        # User from X-Hubmap-User header (set by hubmap-auth after authorization)
        # Falls back to '-' if not authenticated
        user = request.headers.get('X-Hubmap-User', '-')
        
        # Request time in AWS/Apache format: [DD/MMM/YYYY:HH:MM:SS +0000]
        request_time = datetime.now(timezone.utc).strftime('%d/%b/%Y:%H:%M:%S +0000')
        
        # HTTP method, path, and protocol
        method = request.method
        resource_path = request.path
        protocol = request.environ.get('SERVER_PROTOCOL', 'HTTP/1.1')
        
        # Response status code
        status = response.status_code
        
        # Response length (content length in bytes)
        response_length = '-'
        if response.content_length:
            response_length = response.content_length
        elif hasattr(response, 'data'):
            response_length = len(response.data)
        
        # Request ID (generated in before_request, or '-' if not available)
        request_id = getattr(g, 'request_id', '-')
        
        # Format log message matching AWS API Gateway custom access log format:
        # $sourceIp $caller $user [$requestTime] "$method $resourcePath $protocol" $status $responseLength $requestId
        log_message = (
            f'{source_ip} {caller} {user} '
            f'[{request_time}] '
            f'"{method} {resource_path} {protocol}" '
            f'{status} {response_length} {request_id}'
        )

        logger.log(level=ENDPOINT_LOG_LEVEL
                   , msg=log_message)
        
        # Must return response unchanged for Flask
        return response
