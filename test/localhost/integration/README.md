# Entity-API Localhost Integration Tests

Integration tests for entity-api localhost deployment with hubmap-auth. These tests verify that entity-api correctly integrates with hubmap-auth for authorization and properly serves requests.

## Test Files

This directory contains tests organized by functionality:

- **test_endpoints_public.py** - Public endpoints (no auth required)
- **test_endpoints_protected.py** - Protected endpoints (auth required)
- **test_authorization.py** - Authorization integration with hubmap-auth
- **test_configuration.py** - nginx and app configuration validation
- **test_cors.py** - CORS headers and preflight
- **test_flask_app.py** - Flask application behavior

Files are named to group together alphabetically by purpose (all `test_endpoints_*` files group together, etc.).

## Prerequisites

### Running Containers

The tests require both hubmap-auth and entity-api to be running:

```bash
# Start hubmap-auth first
cd gateway
./docker-localhost.sh build
./docker-localhost.sh start

# Verify it's healthy
docker ps | grep hubmap-auth  # Should show "healthy"

# Start entity-api
cd entity-api/docker
./docker-localhost.sh build
./docker-localhost.sh start

# Verify it's healthy
docker ps | grep entity-api  # Should show "healthy"
```

### Python Environment

Tests use the same dependencies as the main application:

```bash
# Create virtual environment (first time only, from entity-api repo root)
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install application dependencies (includes requests)
pip install -r src/requirements.txt

# Suppress pip upgrade notices (optional)
export PIP_DISABLE_PIP_VERSION_CHECK=1
```

## Running the Tests

### From entity-api repository root

```bash
# Activate virtual environment
source .venv/bin/activate

# Run all localhost integration tests
python -m unittest discover -s test/localhost/integration -p "test_*.py" -v
```

### Run specific test file

```bash
source .venv/bin/activate

# Run all public endpoint tests
python -m unittest test.localhost.integration.test_endpoints_public -v

# Run all protected endpoint tests
python -m unittest test.localhost.integration.test_endpoints_protected -v

# Run all authorization tests
python -m unittest test.localhost.integration.test_authorization -v
```

### Run Specific Test Classes

```bash
source .venv/bin/activate

# Run just public GET endpoint tests
python -m unittest test.localhost.integration.test_endpoints_public.EndpointsGETPublicTests -v

# Run just protected POST endpoint tests
python -m unittest test.localhost.integration.test_endpoints_protected.EndpointsPOSTProtectedTests -v

# Run just nginx integration tests
python -m unittest test.localhost.integration.test_authorization.NginxAuthRequestTests -v
```

### Run Individual Tests

```bash
source .venv/bin/activate
python -m unittest test.localhost.integration.test_endpoints_public.EndpointsGETPublicTests.test_status_endpoint -v
```

### Run with Verbose Output

Add `-v` flag for detailed output showing each test:

```bash
python -m unittest discover -s test/localhost/integration -p "test_*.py" -v
```

### Run with Summary Output

Remove `-v` flag for just pass/fail summary:

```bash
python -m unittest discover -s test/localhost/integration -p "test_*.py"
```

Output will be:
```
......................
----------------------------------------------------------------------
Ran 22 tests in 3.456s

OK
```

## Test Structure

### Test Classes

**EndpointsGETPublicTests**
- Public GET endpoints accessible without authentication
- Status, entity lookups, provenance, etc.

**EndpointsGETProtectedTests**
- Protected GET endpoints requiring authentication
- Usergroups, unpublished datasets, etc.

**EndpointsPOSTProtectedTests**
- Protected POST endpoints requiring authentication
- Entity creation, dataset components, etc.

**EndpointsPUTProtectedTests**
- Protected PUT endpoints requiring authentication
- Entity updates, dataset retraction, etc.

**EndpointsDELETEProtectedTests**
- Protected DELETE endpoints requiring authentication
- Cache management, etc.

**NginxAuthRequestTests**
- Verifies nginx correctly calls hubmap-auth
- Tests header passing to authorization service

**FlaskApplicationTests**
- Tests Flask app responses after authorization
- Validates 404 handling for undefined routes

## Best Practices Used

### Code Quality
- **Type hints** - All parameters and return types annotated for clarity
- **Docstrings** - Every test has descriptive documentation
- **Descriptive names** - Test names clearly describe what they verify
- **Proper assertions** - Meaningful assertion messages for failures

### Test Organization
- **Class-level constants** - `BASE_URL`, `TIMEOUT` defined once and reused
- **setUpClass** - Expensive setup (container checks) run once per class
- **subTest** - Parameterized tests provide clear failure reporting per endpoint
- **Focused tests** - Each test validates one specific behavior

### Robustness
- **Timeout handling** - All requests have explicit timeouts
- **Connection error handling** - Graceful failure with helpful messages
- **Conditional skipping** - Tests skip gracefully when containers unavailable
- **Clear error messages** - Failures indicate exactly what went wrong and how to fix

### CI/CD Ready
- **No extra dependencies** - Uses application's existing requirements.txt
- **Subprocess isolation** - Docker commands use subprocess with timeout
- **Exit codes** - Proper test success/failure reporting
- **Environment agnostic** - Works in local development and CI pipelines

## Test Coverage

### What These Tests Verify

✅ entity-api container starts and becomes healthy  
✅ nginx integrates with hubmap-auth via auth_request  
✅ Public endpoints accessible without authentication  
✅ Protected endpoints block access without authentication  
✅ Flask application handles authorized requests  
✅ 404 returned for undefined routes  
✅ CORS headers properly configured  
✅ Docker network communication works  
✅ Authorization headers passed correctly  

### What These Tests Don't Cover

❌ Token validation with real Globus tokens (requires valid credentials)  
❌ Group membership validation (requires test users in specific groups)  
❌ Database operations (Neo4j integration)  
❌ Load testing / performance under stress  
❌ Security penetration testing  

## Troubleshooting

### "Cannot connect to entity-api"
**Cause:** Container not running or not accessible  
**Solution:**
```bash
cd docker
./docker-localhost.sh start
docker ps | grep entity-api
```

### "entity-api not ready: status returned 401"
**Cause:** hubmap-auth not running or misconfigured  
**Solution:** Start hubmap-auth first
```bash
cd gateway
./docker-localhost.sh start
docker ps | grep hubmap-auth
```

### "Authorization Required" on public endpoints
**Cause:** api_endpoints.localhost.json misconfigured or nginx not passing correct Host header  
**Solution:** Check configuration
```bash
docker exec hubmap-auth cat /usr/src/app/api_endpoints.json | grep entity-api
docker exec entity-api cat /etc/nginx/conf.d/entity-api.conf | grep "proxy_set_header Host"
```

### Tests hang or timeout
**Cause:** Network connectivity issues between containers  
**Solution:** Verify both containers on same network
```bash
docker network inspect gateway_hubmap | grep -E "hubmap-auth|entity-api"
```

## Future Enhancements

### Pytest Migration (Optional)

While these tests use Python's built-in `unittest`, you can optionally migrate to pytest for additional features:

**Benefits of pytest:**
- More concise syntax with simple `assert` statements
- Better parameterized testing with `@pytest.mark.parametrize`
- Richer output formatting and failure reporting
- Extensive plugin ecosystem (coverage, parallel execution, etc.)
- Fixture system for complex setup/teardown

**Recommendation:** Stick with unittest for now unless you need pytest-specific features. Unittest is part of Python's standard library and sufficient for these integration tests.

## Contributing

When adding new tests:

1. **Follow existing patterns** - Use the same class structure and naming conventions
2. **Add docstrings** - Every test should explain what it validates
3. **Use subTest for parameters** - When testing multiple similar cases
4. **Handle failures gracefully** - Provide actionable error messages
5. **Keep tests independent** - Each test should work in isolation
6. **Update this README** - Document new test files or significant changes

## CI/CD Integration

These tests are designed to run in GitHub Actions. See the parent [test/README.md](../README.md) for example workflow configuration.

## Related Documentation

- [Parent Test Suite Overview](../README.md)
- [Entity-API Deployment Guide](../../README.md)
- [Gateway API Endpoints Configuration](../../../gateway/api_endpoints.localhost.json)
- [Gateway Test Suite](../../../gateway/test/README.md)
