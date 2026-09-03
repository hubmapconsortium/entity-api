# Localhost Testing for Entity-API

This directory contains tests for entity-api running in Docker Desktop on localhost. These tests verify the service works correctly in a local development/proof-of-concept environment and properly integrates with hubmap-auth for authorization.

## Purpose

Localhost tests serve multiple purposes:

1. **Pre-deployment verification** - Validate configuration changes before pushing to DEV
2. **Authorization integration** - Verify entity-api correctly uses hubmap-auth
3. **Proof-of-concept** - Demonstrate entity-api deployment without AWS infrastructure
4. **Regression testing** - Ensure changes don't break existing functionality

## Test Types

### Integration Tests (`integration/`)

End-to-end tests that verify entity-api integrates correctly with hubmap-auth over Docker networking.

**What they test:**
- Container startup and health
- nginx auth_request integration with hubmap-auth
- Public endpoints accessible without auth
- Protected endpoints require proper authorization
- Flask application responses
- Docker network connectivity

**See:** [integration/README.md](integration/README.md)

### Performance Tests (`performance/`) - Future

Benchmarks and load tests for localhost deployment.

**What they will test:**
- Response time under load
- Concurrent request handling
- Database query performance
- Memory usage patterns

## Prerequisites

### 1. Docker Setup

Create the shared Docker network (one-time setup):
```bash
docker network create gateway_hubmap
```

### 2. Build and Start Containers

```bash
# Start hubmap-auth first (entity-api depends on it)
cd gateway
./docker-localhost.sh build
./docker-localhost.sh start

# Wait for healthy status
docker ps | grep hubmap-auth  # Should show "healthy"

# Then start entity-api
cd entity-api/docker
./docker-localhost.sh build
./docker-localhost.sh start

# Verify both containers are healthy
docker ps | grep -E "hubmap-auth|entity-api"
```

### 3. Python Environment

Tests use the same dependencies as the main application:

```bash
# Create virtual environment (first time only, from entity-api repo root)
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install application dependencies (includes requests)
pip install -r src/requirements.txt
```

## Running Tests

### All Localhost Tests

```bash
source .venv/bin/activate
python -m unittest discover -s test/localhost -v
```

### Integration Tests Only

```bash
source .venv/bin/activate
python -m unittest discover -s test/localhost/integration -v
```

### Specific Test File

```bash
source .venv/bin/activate
python -m unittest test.localhost.integration.test_endpoints_public -v
```

## Environment Differences

Localhost deployment differs from higher tiers in several ways:

| Aspect | Localhost | DEV/TEST/PROD |
|--------|-----------|---------------|
| Authorization | hubmap-auth (Docker) | AWS API Gateway + Lambda |
| SSL/TLS | Disabled | Let's Encrypt certificates |
| Ports | 3333 (custom) | 8080 (standard) |
| Logging | Local files + Docker logs | CloudWatch Logs |
| Network | `gateway_hubmap` (Docker) | AWS VPC |
| Database | Local Neo4j or remote | AWS-hosted Neo4j |

Tests in this directory account for these differences.

## Debugging Failed Tests

### Container Not Running

```bash
# Check container status
docker ps -a | grep entity-api

# Check logs
docker logs entity-api

# Restart if needed
cd docker
./docker-localhost.sh down
./docker-localhost.sh start
```

### Container Not Healthy

```bash
# Check health status
docker inspect entity-api | grep -A 10 Health

# Common causes:
# - Port 3333 already in use
# - nginx configuration error
# - Cannot reach hubmap-auth
# - Flask app.cfg missing
```

### Authorization Failures

```bash
# Verify hubmap-auth is running and healthy
docker ps | grep hubmap-auth

# Test entity-api can reach hubmap-auth
docker exec entity-api curl http://hubmap-auth:7777/status.json

# Check entity-api nginx logs for auth requests
docker exec entity-api cat /usr/src/app/log/nginx_access_entity-api.log | tail -20
```

### Connection Refused

```bash
# Verify port mapping
docker port entity-api

# Test from host
curl http://localhost:3333/status

# Test from inside container
docker exec entity-api curl http://localhost:8080/status
```

### Docker Network Issues

```bash
# Inspect network
docker network inspect gateway_hubmap

# Verify both containers are on the network
docker network inspect gateway_hubmap | grep -E "hubmap-auth|entity-api"
```

## Adding New Test Types

When adding new test categories:

1. **Create subdirectory** under `test/localhost/`
2. **Add README.md** explaining the test type and how to run
3. **Update this README** to document the new test type
4. **Follow best practices** from existing integration tests

## Related Documentation

- [Parent Test Suite Overview](../README.md)
- [Docker Localhost Deployment](../../docker/README.md)
- [Gateway API Endpoints Configuration](../../../gateway/api_endpoints.localhost.json)
- [Gateway Test Suite](../../../gateway/test/README.md)
