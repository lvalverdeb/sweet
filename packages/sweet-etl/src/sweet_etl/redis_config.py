"""Redis connection profile.

No existing boti/boti_data/boti_dask primitive to wrap here — checked, zero
Redis references anywhere in the ecosystem — so this is a new, small config
object, following the same shape as `FilesystemConfig`/`SqlDatabaseConfig`.
"""

from __future__ import annotations

from pydantic import BaseModel, SecretStr


class RedisConfig(BaseModel):
    host: str
    port: int = 6379
    db: int = 0
    decode_responses: bool = False
    password: SecretStr | None = None
