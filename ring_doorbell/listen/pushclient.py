"""FcmPushClient that survives Web Push headers firebase-messaging cannot parse.

firebase-messaging 0.4.x slices the ``dh=`` and ``salt=`` prefixes off the
``crypto-key`` and ``encryption`` headers by position and decodes the rest. Some
Ring notifications carry a second ``p256ecdsa=`` parameter in ``crypto-key``,
which leaves it attached to the key, fails to decode and shuts the client down
permanently. Remove this once a firebase-messaging release reads the headers by
name (sdb9696/firebase-messaging#43).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from firebase_messaging import FcmPushClient

if TYPE_CHECKING:
    from firebase_messaging.proto.mcs_pb2 import DataMessageStanza

_logger = logging.getLogger(__name__)

# The app data keys holding Web Push headers, and the parameter needed from each
_WEBPUSH_HEADER_PARAMS = {"crypto-key": "dh", "encryption": "salt"}


def webpush_header_param(header: str, name: str) -> str:
    """Return a parameter of a ``Crypto-Key`` or ``Encryption`` header.

    Both headers are ``;`` separated parameter lists in no fixed order
    (RFC 8188 section 2.1, RFC 8291 section 4). Values are base64url and may
    arrive without ``=`` padding, which is restored so they decode.
    """
    for segment in header.split(";"):
        param, sep, value = segment.strip().partition("=")
        if sep and param == name:
            value = value.strip()
            return value + "=" * (-len(value) % 4)
    msg = f"No {name} parameter in web push header"
    raise ValueError(msg)


class RingFcmPushClient(FcmPushClient):
    """FcmPushClient that reads Web Push header parameters by name."""

    def _app_data_by_key(
        self,
        p: DataMessageStanza,
        key: str,
        do_not_raise: bool = False,  # noqa: FBT001, FBT002
    ) -> str:
        value = super()._app_data_by_key(p, key, do_not_raise)
        if value and (param := _WEBPUSH_HEADER_PARAMS.get(key)):
            # The base class strips the "<param>=" prefix by position
            return f"{param}={webpush_header_param(value, param)}"
        return value

    def _handle_data_message(self, msg: DataMessageStanza) -> None:
        # The base class shuts the client down permanently on any exception
        # here, so a message that cannot be decrypted is skipped instead.
        try:
            super()._handle_data_message(msg)
        except Exception:
            _logger.exception(
                "Unable to decrypt push notification %s, skipping it",
                msg.persistent_id,
            )
