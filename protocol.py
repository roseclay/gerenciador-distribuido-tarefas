"""Definições e utilitários do protocolo GDT/1.0.

Cada mensagem é um objeto JSON codificado em UTF-8 e terminado por ``\n``.
Este módulo é compartilhado pelo cliente e pelo servidor para evitar que a
serialização e as validações básicas sejam implementadas de formas diferentes.
"""

from __future__ import annotations

import json
from typing import Any, BinaryIO


PROTOCOL_NAME = "GDT/1.0"
MAX_MESSAGE_SIZE = 65_536


class ProtocolError(ValueError):
    """Indica uma mensagem que não obedece ao protocolo GDT/1.0."""


def make_message(
    message_type: str,
    request_id: str | None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Cria um envelope de mensagem válido."""
    return {
        "protocol": PROTOCOL_NAME,
        "type": message_type,
        "request_id": request_id,
        "payload": payload or {},
    }


def encode_message(message: dict[str, Any]) -> bytes:
    """Serializa uma mensagem e acrescenta o delimitador de quadro."""
    try:
        encoded = json.dumps(
            message, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"mensagem não serializável: {exc}") from exc

    if len(encoded) > MAX_MESSAGE_SIZE:
        raise ProtocolError("mensagem excede o limite de 65536 bytes")
    return encoded


def decode_message(raw_line: bytes) -> dict[str, Any]:
    """Decodifica e valida a estrutura comum de uma mensagem recebida."""
    if not raw_line.endswith(b"\n"):
        raise ProtocolError("mensagem sem delimitador de fim de quadro")
    if len(raw_line) > MAX_MESSAGE_SIZE:
        raise ProtocolError("mensagem excede o limite de 65536 bytes")

    try:
        message = json.loads(raw_line.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ProtocolError("mensagem não está em UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise ProtocolError("JSON inválido") from exc

    if not isinstance(message, dict):
        raise ProtocolError("a raiz da mensagem deve ser um objeto JSON")
    if message.get("protocol") != PROTOCOL_NAME:
        raise ProtocolError(f"protocolo esperado: {PROTOCOL_NAME}")
    if not isinstance(message.get("type"), str) or not message["type"]:
        raise ProtocolError("campo 'type' ausente ou inválido")
    if message.get("request_id") is not None and not isinstance(
        message["request_id"], str
    ):
        raise ProtocolError("campo 'request_id' deve ser texto ou null")
    if not isinstance(message.get("payload"), dict):
        raise ProtocolError("campo 'payload' deve ser um objeto JSON")
    return message


def read_message(stream: BinaryIO) -> dict[str, Any] | None:
    """Lê exatamente uma mensagem de um fluxo binário.

    Retorna ``None`` quando o outro lado fecha a conexão de forma limpa.
    """
    raw_line = stream.readline(MAX_MESSAGE_SIZE + 1)
    if raw_line == b"":
        return None
    if len(raw_line) > MAX_MESSAGE_SIZE:
        raise ProtocolError("mensagem excede o limite de 65536 bytes")
    return decode_message(raw_line)
