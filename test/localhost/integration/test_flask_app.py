"""
Tests for entity-api Flask application behavior.

These tests verify Flask-specific functionality including error handling,
404 responses for undefined routes, and application-level logic.

Run all Flask app tests:
    python -m unittest test.localhost.integration.test_flask_app -v
"""

import unittest
import requests


class FlaskErrorHandlingTests(unittest.TestCase):
    """Test Flask application error handling."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_undefined_endpoint_returns_404(self):
        """Test that undefined endpoints return 404 from Flask."""
        response = requests.get(
            f"{self.BASE_URL}/this-endpoint-does-not-exist",
            timeout=self.TIMEOUT
        )
        
        # Catch-all in api_endpoints.json should allow through to Flask
        # Flask should return 404 for undefined routes
        self.assertEqual(response.status_code, 404)

    def test_undefined_post_endpoint_returns_404(self):
        """Test that undefined POST endpoints return 404."""
        response = requests.post(
            f"{self.BASE_URL}/undefined-post-endpoint",
            json={"test": "data"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 404)

    def test_undefined_put_endpoint_returns_404(self):
        """Test that undefined PUT endpoints return 404."""
        response = requests.put(
            f"{self.BASE_URL}/undefined-put-endpoint",
            json={"test": "data"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 404)

    def test_undefined_delete_endpoint_returns_404(self):
        """Test that undefined DELETE endpoints return 404."""
        response = requests.delete(
            f"{self.BASE_URL}/undefined-delete-endpoint",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 404)

    def test_malformed_uuid_handled_gracefully(self):
        """Test that malformed UUIDs are handled with proper error codes."""
        # Send request with clearly invalid UUID format
        response = requests.get(
            f"{self.BASE_URL}/entities/not-a-valid-uuid",
            timeout=self.TIMEOUT
        )
        
        # Should return 400 (bad request) or 404 (not found), not crash
        self.assertIn(response.status_code, [400, 404])


class FlaskResponseTests(unittest.TestCase):
    """Test Flask application responses."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_status_returns_valid_json(self):
        """Test that /status returns valid JSON structure."""
        response = requests.get(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 200)
        
        # Should be valid JSON
        data = response.json()
        self.assertIsInstance(data, dict)

    def test_entity_types_returns_list_or_dict(self):
        """Test that /entity-types returns proper data structure."""
        response = requests.get(f"{self.BASE_URL}/entity-types", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        # Entity types can be returned as list or dict depending on implementation
        self.assertIsInstance(data, (list, dict))

    def test_flask_handles_large_payloads(self):
        """Test that Flask handles large request payloads."""
        # Send moderately large payload (under nginx 10M limit)
        large_payload = {"data": "x" * 100000}  # ~100KB
        
        response = requests.post(
            f"{self.BASE_URL}/constraints",
            json=large_payload,
            timeout=self.TIMEOUT
        )
        
        # Should not return 413 (payload too large) for reasonable sizes
        # May return 400 (bad request) or other application errors
        self.assertNotEqual(response.status_code, 413)


class FlaskPerformanceTests(unittest.TestCase):
    """Test Flask application performance characteristics."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_status_endpoint_fast_response(self):
        """Test that status endpoint responds quickly."""
        response = requests.get(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        # Status endpoint should be fast (< 1 second)
        self.assertLess(response.elapsed.total_seconds(), 1.0)

    def test_simple_lookup_reasonable_time(self):
        """Test that simple lookups complete in reasonable time."""
        response = requests.get(
            f"{self.BASE_URL}/entity-types",
            timeout=self.TIMEOUT
        )
        
        # Should complete in under 2 seconds
        self.assertLess(response.elapsed.total_seconds(), 2.0)


if __name__ == "__main__":
    unittest.main()
