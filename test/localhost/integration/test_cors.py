"""
Tests for CORS (Cross-Origin Resource Sharing) configuration in entity-api.

CORS headers enable web browsers to make requests to the API from
different origins. These tests verify proper CORS configuration in nginx.

Run all CORS tests:
    python -m unittest test.localhost.integration.test_cors -v
"""

import unittest
import requests


class CORSHeaderTests(unittest.TestCase):
    """Test CORS headers on entity-api responses."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_cors_allow_origin_header(self):
        """Test that Access-Control-Allow-Origin header is set to *."""
        response = requests.get(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn("Access-Control-Allow-Origin", response.headers)
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "*")

    def test_cors_allow_methods_header(self):
        """Test that Access-Control-Allow-Methods header is present."""
        response = requests.get(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertIn("Access-Control-Allow-Methods", response.headers)
        allowed_methods = response.headers["Access-Control-Allow-Methods"]
        
        # Should include common methods
        self.assertIn("GET", allowed_methods)
        self.assertIn("POST", allowed_methods)

    def test_cors_allow_headers(self):
        """Test that Access-Control-Allow-Headers includes required headers."""
        response = requests.get(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertIn("Access-Control-Allow-Headers", response.headers)
        allowed_headers = response.headers["Access-Control-Allow-Headers"]
        
        # Should include Authorization header for token-based auth
        self.assertIn("Authorization", allowed_headers)

    def test_cors_headers_on_protected_endpoints(self):
        """Test that CORS headers are present even on 401 responses."""
        response = requests.get(f"{self.BASE_URL}/usergroups", timeout=self.TIMEOUT)
        
        # Should return 401 but still have CORS headers
        self.assertEqual(response.status_code, 401)
        self.assertIn("Access-Control-Allow-Origin", response.headers)


class CORSPreflightTests(unittest.TestCase):
    """Test CORS preflight OPTIONS requests."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_options_request_returns_204(self):
        """Test that OPTIONS requests return 204 No Content."""
        response = requests.options(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 204)

    def test_options_includes_allow_methods(self):
        """Test that OPTIONS response includes allowed methods."""
        response = requests.options(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 204)
        self.assertIn("Access-Control-Allow-Methods", response.headers)

    def test_options_includes_allow_headers(self):
        """Test that OPTIONS response includes allowed headers."""
        response = requests.options(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertIn("Access-Control-Allow-Headers", response.headers)

    def test_options_includes_max_age(self):
        """Test that OPTIONS response includes max age for caching."""
        response = requests.options(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertIn("Access-Control-Max-Age", response.headers)
        # Should be 86400 (24 hours) per nginx config
        self.assertEqual(response.headers["Access-Control-Max-Age"], "86400")


if __name__ == "__main__":
    unittest.main()
