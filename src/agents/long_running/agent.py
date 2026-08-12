from src.checkpoint.store import CheckpointStore


class LongRunningAgent:
    """
    Long-running agent with durable PostgreSQL checkpointing.

    The agent can resume a task from the last successfully
    persisted checkpoint after a crash or restart.
    """

    def __init__(self, checkpoint_store=None):
        self.checkpoint_store = (
            checkpoint_store
            or CheckpointStore()
        )

        self.checkpoint_store.initialize()

    def process(
        self,
        task_id,
        total_steps=5,
        fail_at_step=None,
    ):
        """
        Process a long-running task.

        If a checkpoint already exists, resume from the
        next step instead of starting from step 1.

        fail_at_step is used only for testing crash recovery.
        """

        checkpoint = self.checkpoint_store.get_checkpoint(
            task_id
        )

        if checkpoint:
            current_step = checkpoint["current_step"]

            print(
                f"Checkpoint found for {task_id}. "
                f"Resuming from step {current_step + 1}."
            )

        else:
            current_step = 0

            print(
                f"No checkpoint found for {task_id}. "
                "Starting from step 1."
            )

            self.checkpoint_store.save_checkpoint(
                task_id=task_id,
                agent_type="long-running-agent",
                status="running",
                current_step=0,
                checkpoint_data={
                    "message": "task started"
                },
            )

        for step in range(
            current_step + 1,
            total_steps + 1,
        ):

            print(
                f"Processing {task_id}: "
                f"step {step}/{total_steps}"
            )

            # Simulated work
            result = {
                "step": step,
                "message": f"step {step} completed",
            }

            # Persist progress after successful work
            self.checkpoint_store.save_checkpoint(
                task_id=task_id,
                agent_type="long-running-agent",
                status="running",
                current_step=step,
                checkpoint_data=result,
            )

            print(
                f"Checkpoint saved at step {step}."
            )

            # Simulate a crash for testing
            if fail_at_step == step:
                raise RuntimeError(
                    f"Simulated crash at step {step}"
                )

        # Task completed
        self.checkpoint_store.save_checkpoint(
            task_id=task_id,
            agent_type="long-running-agent",
            status="completed",
            current_step=total_steps,
            checkpoint_data={
                "message": "task completed"
            },
        )

        print(
            f"Task {task_id} completed successfully."
        )

        return self.checkpoint_store.get_checkpoint(
            task_id
        )