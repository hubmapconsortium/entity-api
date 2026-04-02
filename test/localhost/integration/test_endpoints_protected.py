"""
Tests for protected entity-api endpoints requiring authentication.

These tests call entity-api endpoints directly and verify they require
proper authentication. No knowledge of hubmap-auth /api_auth internals.

Run all protected endpoint tests:
    python -m unittest test.localhost.integration.test_endpoints_protected -v

Run specific HTTP method tests:
    python -m unittest test.localhost.integration.test_endpoints_protected.EndpointsGETProtectedTests -v
"""

import unittest
import requests


class EndpointsGETProtectedTests(unittest.TestCase):
    """Test protected GET endpoints - authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_usergroups_requires_auth(self):
        """Test GET /usergroups returns 401 without token."""
        response = requests.get(f"{self.BASE_URL}/usergroups", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 401)

    def test_datasets_unpublished_requires_auth(self):
        """Test GET /datasets/unpublished returns 401 without token."""
        response = requests.get(
            f"{self.BASE_URL}/datasets/unpublished",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_descendants_requires_auth(self):
        """Test GET /descendants/<id> returns 401 without token."""
        response = requests.get(
            f"{self.BASE_URL}/descendants/test-id",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_children_requires_auth(self):
        """Test GET /children/<id> returns 401 without token."""
        response = requests.get(
            f"{self.BASE_URL}/children/test-id",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_previous_revisions_requires_auth(self):
        """Test GET /previous_revisions/<id> returns 401 without token."""
        response = requests.get(
            f"{self.BASE_URL}/previous_revisions/test-id",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_next_revisions_requires_auth(self):
        """Test GET /next_revisions/<id> returns 401 without token."""
        response = requests.get(
            f"{self.BASE_URL}/next_revisions/test-id",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)


class EndpointsPOSTProtectedTests(unittest.TestCase):
    """Test protected POST endpoints - authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_entities_create_requires_auth(self):
        """Test POST /entities/<type> returns 401 without token."""
        response = requests.post(
            f"{self.BASE_URL}/entities/sample",
            json={"direct_ancestor_uuid": "test-uuid"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_datasets_components_requires_auth(self):
        """Test POST /datasets/components returns 401 without token."""
        response = requests.post(
            f"{self.BASE_URL}/datasets/components",
            json={"test": "data"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_entities_multiple_samples_requires_auth(self):
        """Test POST /entities/multiple-samples/<count> returns 401 without token."""
        response = requests.post(
            f"{self.BASE_URL}/entities/multiple-samples/5",
            json={"direct_ancestor_uuid": "test-uuid"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)


class EndpointsPUTProtectedTests(unittest.TestCase):
    """Test protected PUT endpoints - authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_entities_update_requires_auth(self):
        """Test PUT /entities/<id> returns 401 without token."""
        response = requests.put(
            f"{self.BASE_URL}/entities/test-uuid",
            json={"description": "updated"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_datasets_retract_requires_admin_auth(self):
        """Test PUT /datasets/<id>/retract returns 401 without admin token."""
        response = requests.put(
            f"{self.BASE_URL}/datasets/test-id/retract",
            json={"retraction_reason": "test reason"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)


class EndpointsDELETEProtectedTests(unittest.TestCase):
    """Test protected DELETE endpoints - authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_flush_cache_requires_auth(self):
        """Test DELETE /flush-cache/<id> returns 401 without token."""
        response = requests.delete(
            f"{self.BASE_URL}/flush-cache/test-id",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)

    def test_flush_all_cache_requires_admin_auth(self):
        """Test DELETE /flush-all-cache returns 401 without admin token."""
        response = requests.delete(
            f"{self.BASE_URL}/flush-all-cache",
            timeout=self.TIMEOUT
        )
        
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
