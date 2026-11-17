"""Servidor TCP concorrente do Gerenciador Distribuído de Tarefas."""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import socketserver
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from protocol import ProtocolError, encode_message, make_message, read_message


LOG = logging.getLogger("gdt.server")


def utc_now() -> str:
    """Retorna instante UTC no formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class TaskStore:
    """Armazena tarefas em memória e as persiste de maneira atômica."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._tasks: dict[int, dict[str, Any]] = {}
        self._next_id = 1
        if path:
            self._load()

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            tasks = data.get("tasks", [])
            if not isinstance(tasks, list):
                raise ValueError("'tasks' deve ser uma lista")
            self._tasks = {int(task["id"]): task for task in tasks}
            self._next_id = max(self._tasks, default=0) + 1
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"não foi possível carregar {self.path}: {exc}") from exc

    def _persist(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {"version": 1, "tasks": list(self._tasks.values())}
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                json.dump(data, temporary, ensure_ascii=False, indent=2)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_name = temporary.name
            os.replace(temporary_name, self.path)
        finally:
            if temporary_name and os.path.exists(temporary_name):
                os.unlink(temporary_name)

    def list_tasks(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(task) for task in sorted(self._tasks.values(), key=lambda t: t["id"])]

    def create(self, title: str, description: str, actor: str) -> dict[str, Any]:
        with self._lock:
            task_id = self._next_id
            self._next_id += 1
            task = {
                "id": task_id,
                "title": title,
                "description": description,
                "completed": False,
                "created_by": actor,
                "created_at": utc_now(),
                "completed_at": None,
            }
            self._tasks[task_id] = task
            self._persist()
            return dict(task)

    def complete(self, task_id: int) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            task["completed"] = True
            task["completed_at"] = utc_now()
            self._persist()
            return dict(task)

    def delete(self, task_id: int) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.pop(task_id, None)
            if task is not None:
                self._persist()
                return dict(task)
            return None


class TaskRequestHandler(socketserver.BaseRequestHandler):
    """Máquina de estados de uma conexão de cliente."""

    server: "DistributedTaskServer"

    def setup(self) -> None:
        self.username: str | None = None
        self._send_lock = threading.Lock()
        self._stream = self.request.makefile("rb")
        self.request.settimeout(self.server.client_timeout)
        LOG.info("conexão aberta por %s:%s", *self.client_address)

    def handle(self) -> None:
        while True:
            try:
                message = read_message(self._stream)
            except socket.timeout:
                self.send_error(None, "TIMEOUT", "conexão encerrada por inatividade")
                return
            except ProtocolError as exc:
                self.send_error(None, "BAD_MESSAGE", str(exc))
                return
            except (ConnectionError, OSError):
                return

            if message is None:
                return

            request_id = message["request_id"]
            if request_id is None:
                self.send_error(None, "INVALID_REQUEST_ID", "requisições exigem request_id")
                continue

            message_type = message["type"].upper()
            if self.username is None:
                if message_type != "HELLO":
                    self.send_error(request_id, "NOT_IDENTIFIED", "envie HELLO primeiro")
                    continue
                self._handle_hello(message)
                continue

            if message_type == "LIST":
                self.send_message(
                    make_message("TASK_LIST", request_id, {"tasks": self.server.store.list_tasks()})
                )
            elif message_type == "CREATE":
                self._handle_create(message)
            elif message_type == "COMPLETE":
                self._handle_complete(message)
            elif message_type == "DELETE":
                self._handle_delete(message)
            elif message_type == "PING":
                self.send_message(make_message("PONG", request_id, {"server_time": utc_now()}))
            elif message_type == "QUIT":
                self.send_message(make_message("BYE", request_id, {"message": "até logo"}))
                return
            elif message_type == "HELLO":
                self.send_error(request_id, "ALREADY_IDENTIFIED", "HELLO já foi processado")
            else:
                self.send_error(request_id, "UNKNOWN_TYPE", f"tipo desconhecido: {message_type}")

    def finish(self) -> None:
        self.server.unregister(self)
        try:
            self._stream.close()
        except OSError:
            pass
        LOG.info("conexão encerrada de %s:%s", *self.client_address)

    def _handle_hello(self, message: dict[str, Any]) -> None:
        username = message["payload"].get("username")
        if not isinstance(username, str) or not (1 <= len(username.strip()) <= 30):
            self.send_error(
                message["request_id"], "INVALID_USERNAME", "username deve ter de 1 a 30 caracteres"
            )
            return
        self.username = username.strip()
        self.server.register(self)
        self.send_message(
            make_message(
                "WELCOME",
                message["request_id"],
                {
                    "username": self.username,
                    "server_time": utc_now(),
                    "capabilities": ["LIST", "CREATE", "COMPLETE", "DELETE", "PING", "QUIT"],
                },
            )
        )

    def _handle_create(self, message: dict[str, Any]) -> None:
        title = message["payload"].get("title")
        description = message["payload"].get("description", "")
        if not isinstance(title, str) or not (1 <= len(title.strip()) <= 100):
            self.send_error(message["request_id"], "INVALID_TITLE", "title deve ter de 1 a 100 caracteres")
            return
        if not isinstance(description, str) or len(description) > 500:
            self.send_error(
                message["request_id"], "INVALID_DESCRIPTION", "description deve ter até 500 caracteres"
            )
            return
        task = self.server.store.create(title.strip(), description.strip(), self.username or "")
        self.send_message(make_message("TASK_CREATED", message["request_id"], {"task": task}))
        self.server.broadcast("created", task, self.username or "")

    def _handle_complete(self, message: dict[str, Any]) -> None:
        task_id = self._valid_task_id(message)
        if task_id is None:
            return
        task = self.server.store.complete(task_id)
        if task is None:
            self.send_error(message["request_id"], "NOT_FOUND", f"tarefa {task_id} não existe")
            return
        self.send_message(make_message("TASK_UPDATED", message["request_id"], {"task": task}))
        self.server.broadcast("completed", task, self.username or "")

    def _handle_delete(self, message: dict[str, Any]) -> None:
        task_id = self._valid_task_id(message)
        if task_id is None:
            return
        task = self.server.store.delete(task_id)
        if task is None:
            self.send_error(message["request_id"], "NOT_FOUND", f"tarefa {task_id} não existe")
            return
        self.send_message(make_message("TASK_DELETED", message["request_id"], {"task": task}))
        self.server.broadcast("deleted", task, self.username or "")

    def _valid_task_id(self, message: dict[str, Any]) -> int | None:
        task_id = message["payload"].get("task_id")
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            self.send_error(message["request_id"], "INVALID_TASK_ID", "task_id deve ser inteiro positivo")
            return None
        return task_id

    def send_message(self, message: dict[str, Any]) -> None:
        data = encode_message(message)
        with self._send_lock:
            self.request.sendall(data)

    def send_error(self, request_id: str | None, code: str, text: str) -> None:
        try:
            self.send_message(make_message("ERROR", request_id, {"code": code, "message": text}))
        except (ConnectionError, OSError):
            pass


class DistributedTaskServer(socketserver.ThreadingTCPServer):
    """Servidor reutilizável, também utilizado pelos testes de integração."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        data_file: Path | None,
        client_timeout: float = 300.0,
    ) -> None:
        self.store = TaskStore(data_file)
        self.client_timeout = client_timeout
        self._clients: set[TaskRequestHandler] = set()
        self._clients_lock = threading.Lock()
        super().__init__(address, TaskRequestHandler)

    def register(self, handler: TaskRequestHandler) -> None:
        with self._clients_lock:
            self._clients.add(handler)

    def unregister(self, handler: TaskRequestHandler) -> None:
        with self._clients_lock:
            self._clients.discard(handler)

    def broadcast(self, event: str, task: dict[str, Any], actor: str) -> None:
        message = make_message(
            "EVENT", None, {"event": event, "task": task, "actor": actor, "occurred_at": utc_now()}
        )
        with self._clients_lock:
            clients = list(self._clients)
        for client in clients:
            try:
                client.send_message(message)
            except (ConnectionError, OSError):
                self.unregister(client)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Servidor do Gerenciador Distribuído de Tarefas")
    parser.add_argument("--host", default="0.0.0.0", help="interface de escuta (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5050, help="porta TCP (padrão: 5050)")
    parser.add_argument(
        "--data-file", type=Path, default=Path("data/tasks.json"), help="arquivo JSON de persistência"
    )
    parser.add_argument("--timeout", type=float, default=300.0, help="inatividade máxima por cliente em segundos")
    parser.add_argument("--verbose", action="store_true", help="ativa logs detalhados")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        with DistributedTaskServer((args.host, args.port), args.data_file, args.timeout) as server:
            host, port = server.server_address
            LOG.info("servidor GDT/1.0 ouvindo em %s:%s", host, port)
            server.serve_forever()
    except KeyboardInterrupt:
        LOG.info("servidor interrompido")
    except (OSError, RuntimeError) as exc:
        raise SystemExit(f"erro ao iniciar servidor: {exc}") from exc


if __name__ == "__main__":
    main()
