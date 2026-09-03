"""
Tests for entity-api authorization integration with hubmap-auth.

These tests verify the nginx ↔ hubmap-auth integration mechanism,
Docker networking, and configuration. Tests here have knowledge of
the authorization infrastructure.

Run all authorization integration tests:
    python -m unittest test.localhost.integration.test_authorization_integration -v
"""

import subprocess
import unittest
import requests


class NginxAuthRequestIntegrationTests(unittest.TestCase):
    """Test nginx auth_request integration with hubmap-auth."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_nginx_config_has_auth_request(self):
        """Test that nginx config includes auth_request directive."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "cat", "/etc/nginx/conf.d/entity-api.conf"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            self.assertIn("auth_request /api_auth", result.stdout)
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect nginx configuration")

    def test_nginx_config_calls_hubmap_auth(self):
        """Test that nginx config proxies to hubmap-auth:7777."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "grep", "-A", "10", 
                 "location = /api_auth", "/etc/nginx/conf.d/entity-api.conf"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            # Should proxy to hubmap-auth
            self.assertIn("hubmap-auth:7777", result.stdout)
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect nginx configuration")

    def test_nginx_sends_correct_host_header(self):
        """Test that nginx sends Host: entity-api to hubmap-auth."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "grep", "proxy_set_header Host", 
                 "/etc/nginx/conf.d/entity-api.conf"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            # Should set Host to "entity-api" not $http_host
            self.assertIn('proxy_set_header Host "entity-api"', result.stdout)
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect nginx configuration")

    def test_nginx_sends_original_uri_header(self):
        """Test that nginx sends X-Original-URI header."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "grep", "X-Original-URI", 
                 "/etc/nginx/conf.d/entity-api.conf"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            self.assertIn("X-Original-URI", result.stdout)
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect nginx configuration")

    def test_nginx_sends_original_method_header(self):
        """Test that nginx sends X-Original-Request-Method header."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "grep", "X-Original-Request-Method", 
                 "/etc/nginx/conf.d/entity-api.conf"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            self.assertIn("X-Original-Request-Method", result.stdout)
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect nginx configuration")


class DockerNetworkConnectivityTests(unittest.TestCase):
    """Test Docker network connectivity between containers."""

    def test_entity_api_can_reach_hubmap_auth(self):
        """Test that entity-api can communicate with hubmap-auth."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "curl", "-f", 
                 "http://hubmap-auth:7777/status.json"],
                capture_output=True,
                text=True,
                timeout=10,
                check=True
            )
            
            self.assertEqual(result.returncode, 0)
            
        except subprocess.CalledProcessError:
            self.fail("entity-api cannot reach hubmap-auth on Docker network")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot test Docker network connectivity")

    def test_containers_on_gateway_hubmap_network(self):
        """Test that both containers are on gateway_hubmap network."""
        try:
            result = subprocess.run(
                ["docker", "network", "inspect", "gateway_hubmap", 
                 "--format", "{{range .Containers}}{{.Name}} {{end}}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            container_names = result.stdout
            self.assertIn("hubmap-auth", container_names)
            self.assertIn("entity-api", container_names)
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect Docker network")

    def test_docker_dns_resolves_hubmap_auth(self):
        """Test that Docker DNS resolves hubmap-auth hostname."""
        try:
            result = subprocess.run(
                ["docker", "exec", "entity-api", "getent", "hosts", "hubmap-auth"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            # Should resolve to an IP address
            self.assertIn("hubmap-auth", result.stdout)
            # Output format: "172.19.0.2    hubmap-auth"
            self.assertRegex(result.stdout, r'\d+\.\d+\.\d+\.\d+')
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot test DNS resolution")


class ContainerHealthTests(unittest.TestCase):
    """Test container health and startup."""

    def test_entity_api_container_healthy(self):
        """Test that entity-api container reports healthy status."""
        try:
            result = subprocess.run(
                ["docker", "inspect", "entity-api", 
                 "--format", "{{.State.Health.Status}}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            health_status = result.stdout.strip()
            self.assertEqual(health_status, "healthy")
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect container health")

    def test_hubmap_auth_container_healthy(self):
        """Test that hubmap-auth container is healthy (prerequisite)."""
        try:
            result = subprocess.run(
                ["docker", "inspect", "hubmap-auth", 
                 "--format", "{{.State.Health.Status}}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            health_status = result.stdout.strip()
            self.assertEqual(
                health_status,
                "healthy",
                "hubmap-auth must be healthy for entity-api tests to work"
            )
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect container health")

    def test_flask_app_loaded_successfully(self):
        """Test that Flask app loaded without configuration errors."""
        try:
            result = subprocess.run(
                ["docker", "logs", "entity-api"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            # Should show WSGI app ready
            self.assertIn("WSGI app 0", result.stdout)
            self.assertIn("ready", result.stdout)
            
            # Should NOT show critical errors
            self.assertNotIn("Unable to load configuration file", result.stdout)
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("Cannot inspect container logs")


if __name__ == "__main__":
    unittest.main()
