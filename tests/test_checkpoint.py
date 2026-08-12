from src.checkpoint.store import CheckpointStore


def test_checkpoint_save_and_resume():

    store = CheckpointStore()

    store.initialize()

    task_id = "TASK-1001"

    store.save_checkpoint(
        task_id=task_id,
        agent_type="long-running-agent",
        status="running",
        current_step=2,
        checkpoint_data={
            "message": "processing document",
            "items_processed": 20,
        },
    )

    checkpoint = store.get_checkpoint(task_id)

    assert checkpoint is not None
    assert checkpoint["task_id"] == task_id
    assert checkpoint["status"] == "running"
    assert checkpoint["current_step"] == 2

    assert checkpoint["checkpoint_data"]["items_processed"] == 20

    store.delete_checkpoint(task_id)