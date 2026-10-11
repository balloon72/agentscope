"""Opt-in PostgreSQL acceptance coverage for ``AsyncSQLAlchemyStorage``.

The regular SQL storage suite intentionally runs against SQLite so it stays
self-contained.  Set ``AGENTSCOPE_TEST_POSTGRES_URL`` to an async SQLAlchemy
TCP URL (for example,
``postgresql+asyncpg://postgres@127.0.0.1:55432/postgres``) when a real
PostgreSQL server is available to exercise the dialect and connection pool.
"""
import os
import uuid
from unittest import skipUnless
from unittest.async_case import IsolatedAsyncioTestCase

from pydantic import SecretStr
from sqlalchemy.engine import make_url

from agentscope.app.storage import AsyncSQLAlchemyStorage
from agentscope.credential import DashScopeCredential

_POSTGRES_URL = os.environ.get("AGENTSCOPE_TEST_POSTGRES_URL", "").strip()


@skipUnless(
    _POSTGRES_URL,
    "set AGENTSCOPE_TEST_POSTGRES_URL to run PostgreSQL acceptance tests",
)
class AsyncSQLAlchemyPostgresAcceptanceTest(IsolatedAsyncioTestCase):
    """Exercise the storage's basic contract against a real PostgreSQL DB."""

    async def test_credentials_and_multiple_connections(self) -> None:
        """Create tables and verify credential CRUD across two engines."""
        parsed_url = make_url(_POSTGRES_URL)
        self.assertEqual(
            parsed_url.drivername,
            "postgresql+asyncpg",
            "AGENTSCOPE_TEST_POSTGRES_URL must use the asyncpg driver",
        )
        self.assertTrue(
            parsed_url.host,
            "AGENTSCOPE_TEST_POSTGRES_URL must use a TCP host, not a socket",
        )

        # Unique user ids keep this opt-in suite safe to run against a shared
        # development database, while create_tables=True verifies idempotent
        # provisioning for each independently-created engine.
        suffix = uuid.uuid4().hex
        owner = f"postgres-acceptance-owner-{suffix}"
        other_user = f"postgres-acceptance-other-{suffix}"

        async with AsyncSQLAlchemyStorage(
            _POSTGRES_URL,
            create_tables=True,
        ) as first:
            credential_id = await first.upsert_credential(
                owner,
                DashScopeCredential(api_key=SecretStr("acceptance-key")),
            )
            self.assertIsNotNone(
                await first.get_credential(owner, credential_id),
            )
            self.assertEqual(
                (await first.get_credential(owner, credential_id)).data[
                    "api_key"
                ],
                "acceptance-key",
            )

            # A second engine proves writes are visible through a separate
            # TCP connection and that create_tables is safe to repeat.
            async with AsyncSQLAlchemyStorage(
                _POSTGRES_URL,
                create_tables=True,
            ) as second:
                self.assertEqual(
                    await second.list_credentials(other_user),
                    [],
                )
                self.assertIsNone(
                    await second.get_credential(other_user, credential_id),
                )
                self.assertFalse(
                    await second.delete_credential(other_user, credential_id),
                )

                rotated = DashScopeCredential(
                    id=credential_id,
                    api_key=SecretStr("rotated-key"),
                )
                self.assertEqual(
                    await second.upsert_credential(owner, rotated),
                    credential_id,
                )

            # A fresh session on the first engine sees the update committed by
            # the second engine, and owner-scoped deletion removes it.
            updated = await first.get_credential(owner, credential_id)
            self.assertIsNotNone(updated)
            self.assertEqual(updated.data["api_key"], "rotated-key")
            self.assertTrue(
                await first.delete_credential(owner, credential_id),
            )
            self.assertIsNone(
                await first.get_credential(owner, credential_id),
            )


if __name__ == "__main__":
    import unittest

    unittest.main()
