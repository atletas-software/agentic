from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken


def _fernet_from_master_key() -> Fernet:
    master = (os.getenv("PLAYER_MEMORY_SETTINGS_MASTER_KEY") or "").strip()
    if not master:
        raise RuntimeError("PLAYER_MEMORY_SETTINGS_MASTER_KEY is required for encrypted admin settings.")
    key = base64.urlsafe_b64encode(hashlib.sha256(master.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_setting_value(plaintext: str) -> str:
    fernet = _fernet_from_master_key()
    return fernet.encrypt((plaintext or "").encode("utf-8")).decode("utf-8")


def decrypt_setting_value(ciphertext: str) -> str:
    fernet = _fernet_from_master_key()
    try:
        return fernet.decrypt((ciphertext or "").encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("Could not decrypt player memory settings payload.") from exc
