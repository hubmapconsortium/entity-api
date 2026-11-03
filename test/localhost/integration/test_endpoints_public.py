"""
Tests for public entity-api endpoints accessible without authentication.

These tests call entity-api endpoints directly and verify responses.
No knowledge of hubmap-auth internal mechanisms.

Run all public endpoint tests:
    python -m unittest test.localhost.integration.test_endpoints_public -v

Run specific HTTP method tests:
    python -m unittest test.localhost.integration.test_endpoints_public.EndpointsGETPublicTests -v
"""

import unittest
import requests
from requests.exceptions import ConnectionError


class EndpointsGETPublicTests(unittest.TestCase):
    """Test public GET endpoints - no authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    @classmethod
    def setUpClass(cls):
        """Verify entity-api is accessible before running tests."""
        try:
            response = requests.get(f"{cls.BASE_URL}/status", timeout=cls.TIMEOUT)
            if response.status_code not in [200, 401]:
                raise RuntimeError(
                    f"entity-api not responding: /status returned {response.status_code}"
                )
        except ConnectionError as e:
            raise RuntimeError(
                f"Cannot connect to entity-api at {cls.BASE_URL}. "
                "Ensure containers are running:\n"
                "  cd gateway && ./docker-localhost.sh start\n"
                "  cd entity-api/docker && ./docker-localhost.sh start"
            ) from e

    def test_status_endpoint(self):
        """Test GET /status returns valid status."""
        response = requests.get(f"{self.BASE_URL}/status", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, dict)

    def test_root_endpoint(self):
        """Test that GET / is publicly accessible."""
        response = requests.get(f"{self.BASE_URL}/", timeout=self.TIMEOUT)

        # Root may require auth based on your config - adjust if needed
        # Currently configured as auth: false in api_endpoints.localhost.json
        self.assertEqual(response.status_code, 200)

    def test_entity_types_endpoint(self):
        """Test GET /entity-types returns entity type information."""
        response = requests.get(f"{self.BASE_URL}/entity-types", timeout=self.TIMEOUT)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, (list, dict))

    def test_entities_lookup(self):
        """Test GET /entities/<id> for entity lookup."""
        test_uuid = "00000000000000000000000000000000"
        response = requests.get(
            f"{self.BASE_URL}/entities/{test_uuid}",
            timeout=self.TIMEOUT
        )
        
        # Should NOT return 401 (endpoint is public)
        # Likely returns 400 or 404 for invalid/non-existent UUID
        self.assertNotEqual(response.status_code, 401)

    def test_provenance_endpoint(self):
        """Test GET /entities/<id>/provenance."""
        test_id = "test-entity-id"
        response = requests.get(
            f"{self.BASE_URL}/entities/{test_id}/provenance",
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_revisions_endpoint(self):
        """Test GET /entities/<id>/revisions."""
        test_id = "test-entity-id"
        response = requests.get(
            f"{self.BASE_URL}/entities/{test_id}/revisions",
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_datasets_sankey_data(self):
        """Test GET /datasets/sankey_data."""
        response = requests.get(
            f"{self.BASE_URL}/datasets/sankey_data",
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_datasets_prov_info(self):
        """Test GET /datasets/<id>/prov-info."""
        test_id = "test-dataset-id"
        response = requests.get(
            f"{self.BASE_URL}/datasets/{test_id}/prov-info",
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_datasets_prov_metadata(self):
        """Test GET /datasets/<id>/prov-metadata."""
        test_id = "test-dataset-id"
        response = requests.get(
            f"{self.BASE_URL}/datasets/{test_id}/prov-metadata",
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_redirect_endpoints(self):
        """Test redirect endpoints are public."""
        test_id = "test-id"
        
        redirect_endpoints = [
            f"/redirect/{test_id}",
            f"/doi/redirect/{test_id}",
            f"/collection/redirect/{test_id}"
        ]
        
        for endpoint in redirect_endpoints:
            with self.subTest(endpoint=endpoint):
                response = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    timeout=self.TIMEOUT,
                    allow_redirects=False
                )
                
                self.assertNotEqual(response.status_code, 401)

    def test_globus_url_endpoints(self):
        """Test Globus URL endpoints are public."""
        test_id = "test-id"
        
        globus_endpoints = [
            f"/entities/{test_id}/globus-url",
            f"/dataset/globus-url/{test_id}",
            f"/entities/dataset/globus-url/{test_id}"
        ]
        
        for endpoint in globus_endpoints:
            with self.subTest(endpoint=endpoint):
                response = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    timeout=self.TIMEOUT
                )
                
                self.assertNotEqual(response.status_code, 401)

    def test_relationship_endpoints(self):
        """Test entity relationship endpoints are public."""
        test_id = "test-entity-id"
        
        relationship_endpoints = [
            f"/entities/{test_id}/tuplets",
            f"/entities/{test_id}/collections",
            f"/entities/{test_id}/uploads",
            f"/entities/{test_id}/siblings",
            f"/entities/{test_id}/ancestor-organs",
            f"/ancestors/{test_id}",
            f"/parents/{test_id}"
        ]
        
        for endpoint in relationship_endpoints:
            with self.subTest(endpoint=endpoint):
                response = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    timeout=self.TIMEOUT
                )
                
                self.assertNotEqual(response.status_code, 401)

    def test_dataset_relationship_endpoints(self):
        """Test dataset relationship endpoints are public."""
        test_id = "test-dataset-id"
        
        dataset_endpoints = [
            f"/datasets/{test_id}/revisions",
            f"/datasets/{test_id}/revision",
            f"/datasets/{test_id}/latest-revision",
            f"/datasets/{test_id}/donors",
            f"/datasets/{test_id}/samples",
            f"/datasets/{test_id}/organs",
            f"/datasets/{test_id}/paired-dataset"
        ]
        
        for endpoint in dataset_endpoints:
            with self.subTest(endpoint=endpoint):
                response = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    timeout=self.TIMEOUT
                )
                
                self.assertNotEqual(response.status_code, 401)

    def test_instanceof_endpoints(self):
        """Test type checking endpoints are public."""
        endpoints = [
            "/entities/type/Sample/instanceof/Entity",
            "/entities/test-id/instanceof/Sample"
        ]
        
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                response = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    timeout=self.TIMEOUT
                )
                
                self.assertNotEqual(response.status_code, 401)

    def test_documents_endpoint(self):
        """Test GET /documents/<id> is public."""
        response = requests.get(
            f"{self.BASE_URL}/documents/test-doc-id",
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)


class EndpointsPOSTPublicTests(unittest.TestCase):
    """Test public POST endpoints - no authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_entities_batch_ids(self):
        """Test POST /entities/batch-ids is public."""
        response = requests.post(
            f"{self.BASE_URL}/entities/batch-ids",
            json={"ids": ["id1", "id2"]},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_constraints_endpoint(self):
        """Test POST /constraints is public."""
        response = requests.post(
            f"{self.BASE_URL}/constraints",
            json={"test": "data"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)


class EndpointsPUTPublicTests(unittest.TestCase):
    """Test public PUT endpoints - no authentication required."""

    BASE_URL = "http://localhost:3333"
    TIMEOUT = 10

    def test_datasets_bulk_update(self):
        """Test PUT /datasets is public."""
        response = requests.put(
            f"{self.BASE_URL}/datasets",
            json={"test": "data"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)

    def test_uploads_update(self):
        """Test PUT /uploads is public."""
        response = requests.put(
            f"{self.BASE_URL}/uploads",
            json={"test": "data"},
            headers={"Content-Type": "application/json"},
            timeout=self.TIMEOUT
        )
        
        self.assertNotEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
