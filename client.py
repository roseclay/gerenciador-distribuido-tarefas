"""Cliente de terminal do Gerenciador Distribuído de Tarefas."""

from __future__ import annotations

import argparse
import queue
import shlex
import socket
import threading
import uuid
from typing import Any

from protocol import ProtocolError, encode_message, make_message, read_message


class ServerError(RuntimeError):
    """Erro de aplicação retornado pelo servidor."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class TaskClient:
    """Cliente programático com correlação de respostas por request_id."""

    def __init__(self, host: str, port: int, timeout: float = 10.0) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket: socket.socket | None = None
        self._stream: Any = None
        self._reader: threading.Thread | None = None
        self._running = threading.Event()
        self._send_lock = threading.Lock()
        self._pending: dict[str, queue.Queue[dict[str, Any]]] = {}
        self._pending_lock = threading.Lock()
        self.events: queue.Queue[dict[str, Any]] = queue.Queue()

    def connect(self, username: str) -> dict[str, Any]:
        if self.socket is not None:
            raise RuntimeError("cliente já conectado")
        self.socket = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.socket.settimeout(None)
        self._stream = self.socket.makefile("rb")
        self._running.set()
        self._reader = threading.Thread(target=self._receive_loop, name="gdt-receiver", daemon=True)
        self._reader.start()
        return self.request("HELLO", {"username": username})

    def request(self, message_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.socket is None or not self._running.is_set():
            raise ConnectionError("cliente não está conectado")
        request_id = uuid.uuid4().hex
        response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = response_queue
        try:
            data = encode_message(make_message(message_type, request_id, payload))
            with self._send_lock:
                self.socket.sendall(data)
            try:
                response = response_queue.get(timeout=self.timeout)
            except queue.Empty as exc:
                raise TimeoutError("o servidor não respondeu dentro do prazo") from exc
            if response["type"] == "ERROR":
                error = response["payload"]
                raise ServerError(error.get("code", "UNKNOWN"), error.get("message", "erro desconhecido"))
            return response
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)

    def close(self, graceful: bool = True) -> None:
        sock = self.socket
        if sock is None:
            return
        if graceful and self._running.is_set():
            try:
                self.request("QUIT")
            except (ConnectionError, OSError, TimeoutError, ServerError):
                pass
        self._running.clear()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
        if self._reader and self._reader is not threading.current_thread():
            self._reader.join(timeout=1)
        self.socket = None

    def _receive_loop(self) -> None:
        try:
            while self._running.is_set():
                message = read_message(self._stream)
                if message is None:
                    break
                request_id = message["request_id"]
                if message["type"] == "EVENT" or request_id is None:
                    self.events.put(message)
                    continue
                with self._pending_lock:
                    destination = self._pending.get(request_id)
                if destination:
                    destination.put(message)
        except (ConnectionError, OSError, ProtocolError):
            pass
        finally:
            self._running.clear()

    def __enter__(self) -> "TaskClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def print_task(task: dict[str, Any]) -> None:
    marker = "✓" if task["completed"] else " "
    description = f" — {task['description']}" if task.get("description") else ""
    print(f"[{marker}] #{task['id']} {task['title']}{description} (por {task['created_by']})")


def print_help() -> None:
    print(
        "Comandos:\n"
        "  listar                         lista todas as tarefas\n"
        "  criar \"título\" [descrição]    cria uma tarefa\n"
        "  concluir ID                   marca uma tarefa como concluída\n"
        "  excluir ID                    remove uma tarefa\n"
        "  ping                          testa a conexão\n"
        "  ajuda                         mostra esta ajuda\n"
        "  sair                          encerra o cliente"
    )


def run_shell(client: TaskClient) -> None:
    print_help()
    while True:
        while True:
            try:
                event = client.events.get_nowait()["payload"]
            except queue.Empty:
                break
            print(f"\n[evento] {event['actor']} {event['event']} a tarefa #{event['task']['id']}")
        try:
            parts = shlex.split(input("gdt> "))
        except (EOFError, KeyboardInterrupt):
            print()
            return
        except ValueError as exc:
            print(f"Comando inválido: {exc}")
            continue
        if not parts:
            continue
        command = parts[0].lower()
        try:
            if command in {"sair", "quit", "exit"}:
                return
            if command in {"ajuda", "help"}:
                print_help()
            elif command in {"listar", "list"}:
                tasks = client.request("LIST")["payload"]["tasks"]
                if not tasks:
                    print("Nenhuma tarefa cadastrada.")
                for task in tasks:
                    print_task(task)
            elif command in {"criar", "create"} and len(parts) >= 2:
                description = " ".join(parts[2:])
                task = client.request("CREATE", {"title": parts[1], "description": description})[
                    "payload"
                ]["task"]
                print("Tarefa criada:")
                print_task(task)
            elif command in {"concluir", "complete"} and len(parts) == 2:
                task = client.request("COMPLETE", {"task_id": int(parts[1])})["payload"]["task"]
                print("Tarefa concluída:")
                print_task(task)
            elif command in {"excluir", "delete"} and len(parts) == 2:
                task = client.request("DELETE", {"task_id": int(parts[1])})["payload"]["task"]
                print(f"Tarefa #{task['id']} excluída.")
            elif command == "ping" and len(parts) == 1:
                response = client.request("PING")
                print(f"Servidor disponível; horário UTC: {response['payload']['server_time']}")
            else:
                print("Comando ou argumentos inválidos. Digite 'ajuda'.")
        except ValueError:
            print("O ID deve ser um número inteiro.")
        except (ServerError, ConnectionError, TimeoutError, OSError) as exc:
            print(f"Erro: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cliente do Gerenciador Distribuído de Tarefas")
    parser.add_argument("--host", default="127.0.0.1", help="endereço do servidor")
    parser.add_argument("--port", type=int, default=5050, help="porta TCP do servidor")
    parser.add_argument("--user", required=True, help="nome do usuário (1 a 30 caracteres)")
    parser.add_argument("--timeout", type=float, default=10.0, help="prazo de resposta em segundos")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = TaskClient(args.host, args.port, args.timeout)
    try:
        welcome = client.connect(args.user)
        print(f"Conectado como {welcome['payload']['username']} ao servidor GDT/1.0.")
        run_shell(client)
    except (ConnectionError, OSError, TimeoutError, ServerError) as exc:
        raise SystemExit(f"não foi possível usar o servidor: {exc}") from exc
    finally:
        client.close()


if __name__ == "__main__":
    main()
