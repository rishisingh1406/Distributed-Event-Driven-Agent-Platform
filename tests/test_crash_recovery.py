import pytest

from src.agents.long_running.agent import LongRunningAgent


def test_resume_after_crash_at_step_2():

    task_id = "TASK-CRASH-002"

    agent = LongRunningAgent()

    # Simulate crash after step 2
    with pytest.raises(RuntimeError):
        agent.process(
            task_id=task_id,
            total_steps=5,
            fail_at_step=2,
        )

    # Create a new agent instance.
    # This simulates the original process crashing
    # and a new worker starting.
    restarted_agent = LongRunningAgent()

    checkpoint = (
        restarted_agent.checkpoint_store
        .get_checkpoint(task_id)
    )

    assert checkpoint is not None

    # Step 2 was successfully persisted
    # before the crash.
    assert checkpoint["current_step"] == 2
    assert checkpoint["status"] == "running"

    # Resume processing.
    result = restarted_agent.process(
        task_id=task_id,
        total_steps=5,
    )

    assert result["current_step"] == 5
    assert result["status"] == "completed"

    restarted_agent.checkpoint_store.delete_checkpoint(
        task_id
    )


def test_resume_after_crash_at_step_4():

    task_id = "TASK-CRASH-004"

    agent = LongRunningAgent()

    # Crash much later in the task.
    with pytest.raises(RuntimeError):
        agent.process(
            task_id=task_id,
            total_steps=6,
            fail_at_step=4,
        )

    # Simulate restart.
    restarted_agent = LongRunningAgent()

    checkpoint = (
        restarted_agent.checkpoint_store
        .get_checkpoint(task_id)
    )

    assert checkpoint is not None
    assert checkpoint["current_step"] == 4

    # Resume from step 5.
    result = restarted_agent.process(
        task_id=task_id,
        total_steps=6,
    )

    assert result["current_step"] == 6
    assert result["status"] == "completed"

    restarted_agent.checkpoint_store.delete_checkpoint(
        task_id
    )


def test_completed_task_is_not_lost():

    task_id = "TASK-COMPLETED-001"

    agent = LongRunningAgent()

    result = agent.process(
        task_id=task_id,
        total_steps=3,
    )

    assert result["status"] == "completed"
    assert result["current_step"] == 3

    # Simulate another process reading the state.
    restarted_agent = LongRunningAgent()

    checkpoint = (
        restarted_agent.checkpoint_store
        .get_checkpoint(task_id)
    )

    assert checkpoint is not None
    assert checkpoint["status"] == "completed"
    assert checkpoint["current_step"] == 3

    restarted_agent.checkpoint_store.delete_checkpoint(
        task_id
    )