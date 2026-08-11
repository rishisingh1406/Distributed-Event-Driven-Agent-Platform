from .processor import DocumentProcessor


class DocumentProcessingAgent:

    def __init__(self):
        self.processor = DocumentProcessor()

    def process(self, event: dict) -> dict:

        payload = event["payload"]

        document_id = payload["document_id"]
        filename = payload["filename"]
        content = payload["content"]

        result = self.processor.process(content)

        return {
            "document_id": document_id,
            "filename": filename,
            "status": "processed",
            "character_count": result["character_count"],
            "chunk_count": result["chunk_count"],
            "chunks": result["chunks"],
        }