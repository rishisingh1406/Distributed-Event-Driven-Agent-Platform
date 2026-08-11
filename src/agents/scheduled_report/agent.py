from datetime import datetime, timezone


class ScheduledReportAgent:

    def process(self, event: dict) -> dict:
        """
        Process a report.scheduled event
        and generate a report result.
        """

        payload = event["payload"]

        report_id = payload["report_id"]
        report_type = payload["report_type"]

        generated_at = datetime.now(
            timezone.utc
        ).isoformat()

        report = {
            "report_id": report_id,
            "report_type": report_type,
            "status": "completed",
            "generated_at": generated_at,
            "summary": (
                f"{report_type} report generated successfully."
            ),
        }

        return report