import time

import streamlit as st

from metrics import (
    get_topics,
    get_latest_offsets,
    get_total_lag,
)


st.set_page_config(
    page_title="Agent Event Platform",
    layout="wide",
)

st.title("Distributed Agent Event Platform")

st.caption(
    "Live monitoring of Redpanda topics and consumer infrastructure"
)


# ---------------------------------------------------------
# Consumer groups
# ---------------------------------------------------------

CONSUMER_GROUPS = {
    "ticket.created": "ticket-triage-group",
}


# ---------------------------------------------------------
# Agent mapping
# ---------------------------------------------------------

AGENTS = {
    "ticket.created": {
        "agent": "Ticket Triage Agent",
        "group": "ticket-triage-group",
    },
    "document.uploaded": {
        "agent": "Document Processing Agent",
        "group": None,
    },
    "report.scheduled": {
        "agent": "Scheduled Report Agent",
        "group": None,
    },
}


# ---------------------------------------------------------
# Session state for throughput
# ---------------------------------------------------------

if "previous_offsets" not in st.session_state:
    st.session_state.previous_offsets = {}

if "previous_time" not in st.session_state:
    st.session_state.previous_time = time.time()


current_time = time.time()

elapsed_seconds = (
    current_time
    - st.session_state.previous_time
)


# ---------------------------------------------------------
# Load topics
# ---------------------------------------------------------

try:

    topics = get_topics()

except Exception as exc:

    st.error(
        f"Unable to connect to Redpanda: {exc}"
    )

    st.stop()


# ---------------------------------------------------------
# Calculate throughput
# ---------------------------------------------------------

throughput = {}

for topic in topics:

    try:

        offsets = get_latest_offsets(topic)

        current_offset = sum(
            data["high"]
            for data in offsets.values()
        )

        previous_offset = (
            st.session_state.previous_offsets
            .get(topic, current_offset)
        )

        if elapsed_seconds > 0:

            events = max(
                current_offset - previous_offset,
                0
            )

            throughput[topic] = (
                events / elapsed_seconds
            )

        else:

            throughput[topic] = 0

        st.session_state.previous_offsets[
            topic
        ] = current_offset

    except Exception:

        throughput[topic] = 0


st.session_state.previous_time = current_time


# ---------------------------------------------------------
# Overview
# ---------------------------------------------------------

st.subheader("System Overview")

col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Topics",
        len(topics)
    )


with col2:

    st.metric(
        "Monitored Consumer Groups",
        len(CONSUMER_GROUPS)
    )


with col3:

    total_lag = 0

    for topic, group_id in CONSUMER_GROUPS.items():

        try:

            total_lag += get_total_lag(
                topic,
                group_id
            )

        except Exception:

            pass

    st.metric(
        "Total Consumer Lag",
        total_lag
    )


with col4:

    total_throughput = sum(
        throughput.values()
    )

    st.metric(
        "Throughput",
        f"{total_throughput:.2f} events/sec"
    )


# ---------------------------------------------------------
# Topic monitoring
# ---------------------------------------------------------

st.subheader("Topic Monitoring")


for topic in topics:

    st.markdown(
        f"### {topic}"
    )

    try:

        offsets = get_latest_offsets(topic)

        partition_count = len(offsets)

        latest_offset = sum(
            data["high"]
            for data in offsets.values()
        )

        topic_throughput = throughput.get(
            topic,
            0
        )

        col1, col2, col3, col4 = st.columns(4)


        with col1:

            st.metric(
                "Partitions",
                partition_count
            )


        with col2:

            st.metric(
                "Latest Offset",
                latest_offset
            )


        with col3:

            if topic in CONSUMER_GROUPS:

                group_id = CONSUMER_GROUPS[
                    topic
                ]

                lag = get_total_lag(
                    topic,
                    group_id
                )

                st.metric(
                    "Consumer Lag",
                    lag
                )

            else:

                st.metric(
                    "Consumer Lag",
                    "N/A"
                )


        with col4:

            st.metric(
                "Throughput",
                f"{topic_throughput:.2f} events/sec"
            )


    except Exception as exc:

        st.error(
            f"Failed to monitor `{topic}`: {exc}"
        )


# ---------------------------------------------------------
# Agent Status
# ---------------------------------------------------------

st.subheader("Agent Status")


for topic, config in AGENTS.items():

    if topic not in topics:
        continue

    agent_name = config["agent"]

    group_id = config["group"]

    if group_id:

        try:

            lag = get_total_lag(
                topic,
                group_id
            )

            if lag == 0:

                status = "RUNNING / CAUGHT UP"

            else:

                status = "RUNNING / PROCESSING"

        except Exception:

            status = "UNKNOWN"

    else:

        status = "MONITORED"


    col1, col2, col3 = st.columns(3)

    with col1:

        st.write(
            f"**Agent:** {agent_name}"
        )

    with col2:

        st.write(
            f"**Topic:** {topic}"
        )

    with col3:

        st.write(
            f"**Status:** {status}"
        )


# ---------------------------------------------------------
# Refresh
# ---------------------------------------------------------

st.divider()

if st.button("Refresh Metrics"):

    st.rerun()