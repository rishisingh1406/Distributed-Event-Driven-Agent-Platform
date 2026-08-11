from src.agents.document_processing.agent import (
    DocumentProcessingAgent,
)


def test_document_processing():

    agent = DocumentProcessingAgent()

    event = {
        "payload": {
            "document_id": "DOC-1001",
            "filename": "company-policy.txt",
            "content": (
                "This is a company policy document. "
                "Employees must follow security procedures."
            ),
        }
    }

    result = agent.process(event)

    assert result["document_id"] == "DOC-1001"
    assert result["filename"] == "company-policy.txt"
    assert result["status"] == "processed"
    assert result["chunk_count"] == 1
    assert result["character_count"] > 0