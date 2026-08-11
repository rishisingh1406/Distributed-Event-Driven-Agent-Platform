class DocumentProcessor:

    def extract_text(self, content: str) -> str:
        """
        Extract text from the uploaded document.

        For Day 60, the uploaded document is represented
        directly as text.
        """

        if not content:
            return ""

        return content.strip()

    def chunk_text(
        self,
        text: str,
        chunk_size: int = 200,
    ) -> list[str]:
        """
        Split document text into simple chunks.
        """

        if not text:
            return []

        words = text.split()

        chunks = []

        for i in range(0, len(words), chunk_size):

            chunk = " ".join(
                words[i:i + chunk_size]
            )

            chunks.append(chunk)

        return chunks

    def process(self, content: str) -> dict:
        """
        Extract text and split it into chunks.
        """

        text = self.extract_text(content)

        chunks = self.chunk_text(text)

        return {
            "text": text,
            "chunks": chunks,
            "chunk_count": len(chunks),
            "character_count": len(text),
        }