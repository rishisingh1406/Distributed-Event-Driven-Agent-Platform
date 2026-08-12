import pytest

from src.agents.long_running.agent import (
    LongRunningAgent,
)


def test_long_running_agent_resumes_after_crash():

    task_id = "TASK-RESUME-1001"

    agent = LongRunningAgent()

    # First execution crashes at step 2
    with pytest.raises(RuntimeError):
        agent.process(
            task_id=task_id,
            total_steps=5,
            fail_at_step=2,
        )

    # Simulate a completely new agent process
    restarted_agent = LongRunningAgent()

    checkpoint = (
        restarted_agent.checkpoint_store
        .get_checkpoint(task_id)
    )

    assert checkpoint is not None
    assert checkpoint["current_step"] == 2
    assert checkpoint["status"] == "running"

    # Resume the task
    result = restarted_agent.process(
        task_id=task_id,
        total_steps=5,
    )

    assert result["current_step"] == 5
    assert result["status"] == "completed"

    # Cleanup
    restarted_agent.checkpoint_store.delete_checkpoint(
        task_id
    )