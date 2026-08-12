import json
import os
from datetime import datetime, timezone

import psycopg2


class CheckpointStore:
    """
    PostgreSQL-backed checkpoint store.

    Persists the progress of long-running agents so they
    can resume after a crash or restart.
    """

    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        user=None,
        password=None,
    ):
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or os.getenv("POSTGRES_PORT", "5432")
        self.database = database or os.getenv("POSTGRES_DB", "memory_db")
        self.user = user or os.getenv("POSTGRES_USER", "postgres")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "postgres")

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
        Create the checkpoint table if it does not already exist.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_checkpoints (
                    task_id TEXT PRIMARY KEY,
                    agent_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    current_step INTEGER NOT NULL,
                    checkpoint_data JSONB,
                    updated_at TIMESTAMPTZ NOT NULL
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    def save_checkpoint(
        self,
        task_id,
        agent_type,
        status,
        current_step,
        checkpoint_data=None,
    ):
        """
        Insert or update a checkpoint.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                INSERT INTO agent_checkpoints (
                    task_id,
                    agent_type,
                    status,
                    current_step,
                    checkpoint_data,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)

                ON CONFLICT (task_id)
                DO UPDATE SET
                    agent_type = EXCLUDED.agent_type,
                    status = EXCLUDED.status,
                    current_step = EXCLUDED.current_step,
                    checkpoint_data = EXCLUDED.checkpoint_data,
                    updated_at = EXCLUDED.updated_at;
                """,
                (
                    task_id,
                    agent_type,
                    status,
                    current_step,
                    json.dumps(checkpoint_data or {}),
                    datetime.now(timezone.utc),
                ),
            )

            connection.commit()

        finally:
            connection.close()

    def get_checkpoint(self, task_id):
        """
        Retrieve the latest checkpoint for a task.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT
                    task_id,
                    agent_type,
                    status,
                    current_step,
                    checkpoint_data,
                    updated_at
                FROM agent_checkpoints
                WHERE task_id = %s;
                """,
                (task_id,),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return {
                "task_id": row[0],
                "agent_type": row[1],
                "status": row[2],
                "current_step": row[3],
                "checkpoint_data": row[4],
                "updated_at": row[5],
            }

        finally:
            connection.close()

    def delete_checkpoint(self, task_id):
        """
        Remove a checkpoint after the task is permanently complete.
        """

        connection = self._connect()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                DELETE FROM agent_checkpoints
                WHERE task_id = %s;
                """,
                (task_id,),
            )

            connection.commit()

        finally:
            connection.close()