"""Tests for the FcmPushClient subclass used by the event listener."""

import json
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode

import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_der_private_key,
)
from firebase_messaging import FcmPushClient, FcmRegisterConfig
from firebase_messaging.proto.mcs_pb2 import DataMessageStanza
from http_ece import encrypt
from ring_doorbell.listen.pushclient import RingFcmPushClient, webpush_header_param

from tests.conftest import load_fixture

PAYLOAD = {"data": {"foo": "bar"}}


def _b64(data: bytes) -> str:
    return urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _credentials() -> dict:
    return json.loads(load_fixture("ring_listen_credentials.json"))


def _message(credentials: dict, crypto_key: str, raw_data: bytes) -> DataMessageStanza:
    msg = DataMessageStanza(persistent_id="0:1234", raw_data=raw_data)
    for key, value in (
        ("crypto-key", crypto_key),
        ("encryption", f"salt={_b64(_SALT)}"),
        ("subtype", credentials["gcm"]["app_id"]),
    ):
        app_data = msg.app_data.add()
        app_data.key = key
        app_data.value = value
    return msg


_SALT = os.urandom(16)


def _encrypt(credentials: dict, sender: ec.EllipticCurvePrivateKey) -> bytes:
    """Encrypt the payload to the listener's keys as a Web Push sender would."""
    receiver = load_der_private_key(
        urlsafe_b64decode(credentials["keys"]["private"] + "========"), password=None
    )
    receiver_public = receiver.public_key().public_bytes(
        Encoding.X962, PublicFormat.UncompressedPoint
    )
    return encrypt(
        json.dumps(PAYLOAD).encode(),
        salt=_SALT,
        private_key=sender,
        dh=receiver_public,
        version="aesgcm",
        auth_secret=urlsafe_b64decode(credentials["keys"]["secret"] + "========"),
    )


def _client(credentials: dict, received: list) -> RingFcmPushClient:
    return RingFcmPushClient(
        lambda notification, _persistent_id, _obj: received.append(notification),
        FcmRegisterConfig("project", "app", "api_key", "sender"),
        credentials,
        None,
    )


@pytest.mark.parametrize(
    ("header", "name", "expected"),
    [
        pytest.param("dh=AAAA", "dh", "AAAA", id="single"),
        pytest.param("salt=BB==", "salt", "BB==", id="padded"),
        pytest.param("dh=AAA", "dh", "AAA=", id="restores_padding"),
        pytest.param("dh=AAAA; p256ecdsa=CCCC", "dh", "AAAA", id="vapid"),
        pytest.param("p256ecdsa=CCCC;dh=AAAA", "dh", "AAAA", id="vapid_first"),
    ],
)
def test_webpush_header_param(header: str, name: str, expected: str) -> None:
    assert webpush_header_param(header, name) == expected


def test_webpush_header_param_missing() -> None:
    with pytest.raises(ValueError, match="No dh parameter"):
        webpush_header_param("p256ecdsa=CCCC", "dh")


@pytest.mark.parametrize(
    "crypto_key",
    [
        pytest.param("dh={dh}", id="dh_only"),
        pytest.param("dh={dh}; p256ecdsa={vapid}", id="vapid"),
        pytest.param("p256ecdsa={vapid};dh={dh}", id="vapid_first"),
    ],
)
def test_decrypts_web_push_headers(crypto_key: str) -> None:
    """Messages are delivered whatever shape the crypto-key header has."""
    credentials = _credentials()
    sender = ec.generate_private_key(ec.SECP256R1())
    sender_public = sender.public_key().public_bytes(
        Encoding.X962, PublicFormat.UncompressedPoint
    )
    vapid = ec.generate_private_key(ec.SECP256R1()).public_key()
    msg = _message(
        credentials,
        crypto_key.format(
            dh=_b64(sender_public),
            vapid=_b64(
                vapid.public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
            ),
        ),
        _encrypt(credentials, sender),
    )
    received: list = []

    _client(credentials, received)._handle_data_message(msg)

    assert received == [PAYLOAD]


def test_vapid_header_breaks_base_client() -> None:
    """The base client cannot decrypt the header shape the subclass handles."""
    credentials = _credentials()
    sender = ec.generate_private_key(ec.SECP256R1())
    sender_public = _b64(
        sender.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    )
    msg = _message(
        credentials,
        f"dh={sender_public}; p256ecdsa={sender_public}",
        _encrypt(credentials, sender),
    )
    base = FcmPushClient(
        lambda *_: None,
        FcmRegisterConfig("project", "app", "api_key", "sender"),
        credentials,
        None,
    )

    with pytest.raises(ValueError):  # noqa: PT011
        base._handle_data_message(msg)


def test_undecryptable_message_is_skipped(caplog: pytest.LogCaptureFixture) -> None:
    """A message that cannot be decrypted is logged instead of raising."""
    credentials = _credentials()
    sender = ec.generate_private_key(ec.SECP256R1())
    sender_public = sender.public_key().public_bytes(
        Encoding.X962, PublicFormat.UncompressedPoint
    )
    msg = _message(credentials, f"dh={_b64(sender_public)}", b"not encrypted")
    received: list = []

    with caplog.at_level(logging.ERROR):
        _client(credentials, received)._handle_data_message(msg)

    assert received == []
    assert "Unable to decrypt push notification 0:1234" in caplog.text
