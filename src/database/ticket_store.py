import json
import os
from datetime import datetime, timezone

import psycopg2


class TicketStore:
    """
    PostgreSQL persistence layer for processed ticket events.

    Stores event identity and processing results so that
    an event can be traced from producer -> Redpanda ->
    consumer -> PostgreSQL.
    """

    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        user=None,
        password=None,
    ):
        self.host = host or os.getenv(
            "POSTGRES_HOST",
            "localhost",
        )

        self.port = port or os.getenv(
            "POSTGRES_PORT",
            "5432",
        )

        self.database = database or os.getenv(
            "POSTGRES_DB",
            "memory_db",
        )

        self.user = user or os.getenv(
            "POSTGRES_USER",
            "postgres",
        )

        self.password = password or os.getenv(
            "POSTGRES_PASSWORD",
            "postgres",
        )

    def _connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def initialize(self):
        """
        Create the ticket_events table if it does not exist.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ticket_events (
                    event_id TEXT PRIMARY KEY,
                    correlation_id TEXT NOT NULL,
                    ticket_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    triage_result JSONB,
                    created_at TIMESTAMPTZ NOT NULL
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    def save_ticket_result(
        self,
        event_id,
        correlation_id,
        ticket_id,
        status,
        triage_result=None,
    ):
        """
        Persist the result of processing a ticket event.

        Uses event_id as the unique identifier so that
        duplicate/redelivered events update the existing
        record instead of creating duplicate rows.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                INSERT INTO ticket_events (
                    event_id,
                    correlation_id,
                    ticket_id,
                    status,
                    triage_result,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)

                ON CONFLICT (event_id)
                DO UPDATE SET
                    correlation_id = EXCLUDED.correlation_id,
                    ticket_id = EXCLUDED.ticket_id,
                    status = EXCLUDED.status,
                    triage_result = EXCLUDED.triage_result;
                """,
                (
                    event_id,
                    correlation_id,
                    ticket_id,
                    status,
                    json.dumps(triage_result or {}),
                    datetime.now(timezone.utc),
                ),
            )

            connection.commit()

        finally:
            connection.close()

    def get_ticket(self, event_id):
        """
        Retrieve a processed ticket event by event_id.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT
                    event_id,
                    correlation_id,
                    ticket_id,
                    status,
                    triage_result,
                    created_at
                FROM ticket_events
                WHERE event_id = %s;
                """,
                (event_id,),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return {
                "event_id": row[0],
                "correlation_id": row[1],
                "ticket_id": row[2],
                "status": row[3],
                "triage_result": row[4],
                "created_at": row[5],
            }

        finally:
            connection.close()

    def get_ticket_result(self, event_id):
        """
        Retrieve a stored ticket processing result.

        This is an alias for get_ticket() so callers can use
        a more explicit method name when verifying processing.
        """

        return self.get_ticket(event_id)