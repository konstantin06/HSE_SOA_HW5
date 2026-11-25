import unittest
from unittest.mock import Mock

from fastapi.testclient import TestClient

from app.producer import app
from tests.sample import event


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.publisher = Mock()
        app.state.publisher = self.publisher
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.data = event("viewer", "film", "VIEW_STARTED")

    def test_receipt_contains_event_id(self):
        response = self.client.post("/events", json=self.data)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json(), {"event_id": self.data["event_id"]})
        sent, = self.publisher.send.call_args.args
        self.assertEqual(str(sent.event_id), self.data["event_id"])
        self.assertEqual(sent.timestamp.utcoffset().total_seconds(), 0)

    def test_invalid_event_does_not_reach_publisher(self):
        response = self.client.post("/events", json={**self.data, "device_type": "CAR"})
        self.assertEqual(response.status_code, 422)
        self.publisher.send.assert_not_called()

    def test_avro_validation_error(self):
        self.publisher.send.side_effect = ValueError()
        self.assertEqual(self.client.post("/events", json=self.data).status_code, 422)

    def test_missing_ack_returns_503(self):
        self.publisher.send.side_effect = ConnectionError()
        with self.assertLogs("cinema", level="ERROR"):
            response = self.client.post("/events", json=self.data)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("event_id", response.json())

    def test_health_depends_on_kafka(self):
        self.publisher.ready.side_effect = (False, True)
        self.assertEqual(self.client.get("/health").status_code, 503)
        self.assertEqual(self.client.get("/health").status_code, 200)
