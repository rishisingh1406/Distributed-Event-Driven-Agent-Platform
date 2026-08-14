from src.database.ticket_store import TicketStore


def test_ticket_store_initialization():
    store = TicketStore()

    store.initialize()

    print(
        "TicketStore PostgreSQL initialization successful."
    )


if __name__ == "__main__":
    test_ticket_store_initialization()