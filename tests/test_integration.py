"""Testes unitários do protocolo e testes de integração TCP."""

from __future__ import annotations

import json
import queue
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from client import ServerError, TaskClient
from protocol import ProtocolError, decode_message, encode_message, make_message, read_message
from server import DistributedTaskServer


class ProtocolTests(unittest.TestCase):
    def test_round_trip_with_unicode(self) -> None:
        original = make_message("CREATE", "abc", {"title": "Revisão de redes"})
        self.assertEqual(decode_message(encode_message(original)), original)

    def test_rejects_wrong_protocol(self) -> None:
        raw = json.dumps(
            {"protocol": "OUTRO/1", "type": "PING", "request_id": "1", "payload": {}}
        ).encode() + b"\n"
        with self.assertRaises(ProtocolError):
            decode_message(raw)


class ServerIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.data_file = Path(self.temporary.name) / "tasks.json"
        self.server = DistributedTaskServer(("127.0.0.1", 0), self.data_file, client_timeout=5)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.host, self.port = self.server.server_address
        self.clients: list[TaskClient] = []

    def tearDown(self) -> None:
        for client in self.clients:
            client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temporary.cleanup()

    def new_client(self, username: str) -> TaskClient:
        client = TaskClient(self.host, self.port, timeout=2)
        client.connect(username)
        self.clients.append(client)
        return client

    def test_complete_workflow_and_event(self) -> None:
        alice = self.new_client("alice")
        bob = self.new_client("bob")

        created = alice.request(
            "CREATE", {"title": "Preparar relatório", "description": "Documentar protocolo"}
        )["payload"]["task"]
        self.assertEqual(created["id"], 1)
        self.assertFalse(created["completed"])

        event = bob.events.get(timeout=2)
        self.assertEqual(event["type"], "EVENT")
        self.assertEqual(event["payload"]["event"], "created")
        self.assertEqual(event["payload"]["actor"], "alice")

        tasks = bob.request("LIST")["payload"]["tasks"]
        self.assertEqual([task["title"] for task in tasks], ["Preparar relatório"])

        completed = bob.request("COMPLETE", {"task_id": 1})["payload"]["task"]
        self.assertTrue(completed["completed"])
        self.assertIsNotNone(completed["completed_at"])

        deleted = alice.request("DELETE", {"task_id": 1})["payload"]["task"]
        self.assertEqual(deleted["id"], 1)
        self.assertEqual(bob.request("LIST")["payload"]["tasks"], [])

    def test_validation_and_not_found_errors(self) -> None:
        client = self.new_client("tester")
        with self.assertRaises(ServerError) as context:
            client.request("CREATE", {"title": "", "description": "inválida"})
        self.assertEqual(context.exception.code, "INVALID_TITLE")

        with self.assertRaises(ServerError) as context:
            client.request("COMPLETE", {"task_id": 999})
        self.assertEqual(context.exception.code, "NOT_FOUND")

    def test_requires_hello_as_first_message(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=2) as raw_client:
            stream = raw_client.makefile("rb")
            raw_client.sendall(encode_message(make_message("LIST", "request-1")))
            response = read_message(stream)
            self.assertIsNotNone(response)
            assert response is not None
            self.assertEqual(response["type"], "ERROR")
            self.assertEqual(response["payload"]["code"], "NOT_IDENTIFIED")

    def test_tasks_are_persisted_to_disk(self) -> None:
        client = self.new_client("alice")
        client.request("CREATE", {"title": "Persistir", "description": "teste"})
        stored = json.loads(self.data_file.read_text(encoding="utf-8"))
        self.assertEqual(stored["tasks"][0]["title"], "Persistir")


if __name__ == "__main__":
    unittest.main()
