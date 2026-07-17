"""
Dscout - Data Engineering Interview Problem
Round 1: Python Pipeline + SQL

Instructions:
    1. Install dependencies: pip install duckdb pandas
    2. Work through Part 1 first - implement process_events()
    3. Once you have valid_events_df, move to Part 2 and write the SQL queries
    4. Run the file at any point to check your output.

Scenario:
    You're a AI data engineer at a user research platform. Researchers run studies
    where participants complete missions. They have tasks like recording a video, answering
    questions, or uploading photos. Each mission completion generates an event.

    Your job:
    - Ingest raw events from the source
    - Clean and validate them
    - Load them into a structure ready for Snowflake
    - Query the cleaned data to answer analytical questions
"""

import pandas as pd
import duckdb

# ─────────────────────────────────────────────
# RAW DATA
# ─────────────────────────────────────────────

raw_events = [
    # Clean records
    {"event_id": "abc123", "participant_id": "p1", "mission_id": "m1", "study_id": "s1", "event_type": "mission_complete", "timestamp": "2024-01-15T10:30:00Z", "duration_seconds": 142},
    {"event_id": "jkl012", "participant_id": "p4", "mission_id": "m3", "study_id": "s2", "event_type": "mission_complete", "timestamp": "2024-01-15T11:00:00Z", "duration_seconds": 210},
    {"event_id": "mno345", "participant_id": "p5", "mission_id": "m2", "study_id": "s1", "event_type": "mission_complete", "timestamp": "2024-01-14T08:00:00Z", "duration_seconds": 175},
    {"event_id": "pqr678", "participant_id": "p1", "mission_id": "m2", "study_id": "s1", "event_type": "mission_complete", "timestamp": "2024-01-14T09:00:00Z", "duration_seconds": 95},
    {"event_id": "stu901", "participant_id": "p3", "mission_id": "m3", "study_id": "s2", "event_type": "mission_complete", "timestamp": "2024-01-15T12:00:00Z", "duration_seconds": 300},
    {"event_id": "vwx234", "participant_id": "p5", "mission_id": "m3", "study_id": "s2", "event_type": "mission_complete", "timestamp": "2024-01-15T13:00:00Z", "duration_seconds": 860},  # long but valid - anomaly to detect in SQL

    # Problem records
    {"event_id": "abc123", "participant_id": "p1", "mission_id": "m1", "study_id": "s1", "event_type": "mission_complete", "timestamp": "2024-01-15T10:30:00Z", "duration_seconds": 142},   # duplicate
    {"event_id": "def456", "participant_id": None, "mission_id": "m2", "study_id": "s1", "event_type": "mission_complete", "timestamp": "2024-01-15T09:00:00Z", "duration_seconds": 98},    # missing participant_id
    {"event_id": "ghi789", "participant_id": "p3", "mission_id": "m1", "study_id": "s2", "event_type": "mission_complete", "timestamp": "not-a-timestamp", "duration_seconds": -5},         # malformed timestamp + negative duration
]


# ─────────────────────────────────────────────
# PART 1: PYTHON PIPELINE
# ─────────────────────────────────────────────
# Implement process_events() below.
#
# Requirements:
#   - Deduplicate records (event_id is the unique key)
#   - Validate that required fields are present and non-null:
#       event_id, participant_id, mission_id, study_id, timestamp
#   - Validate that timestamp is a parseable ISO 8601 datetime
#   - Validate that duration_seconds is a positive integer
#   - Return two lists:
#       valid_events   - clean records ready for Snowflake
#       rejected_events - records that failed, each with a 'rejection_reason' field

def process_events(raw_events: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Clean and validate raw mission completion events.

    Returns:
        valid_events: list of clean records ready for Snowflake
        rejected_events: list of records that failed validation, each with a 'rejection_reason' field
    """
    # YOUR CODE HERE
    pass


# ─────────────────────────────────────────────
# RUN PART 1
# ─────────────────────────────────────────────

valid_events, rejected_events = process_events(raw_events)

print(f"\n✓ Valid events: {len(valid_events)}")
print(f"✗ Rejected events: {len(rejected_events)}")

if rejected_events:
    print("\nRejected:")
    for r in rejected_events:
        print(f"  - {r.get('event_id', 'unknown')}: {r.get('rejection_reason')}")

# Convert to DataFrame for Part 2
valid_events_df = pd.DataFrame(valid_events)
print(f"\nDataFrame shape: {valid_events_df.shape}")
print(valid_events_df)


# ─────────────────────────────────────────────
# PART 2: SQL
# ─────────────────────────────────────────────
# Using DuckDB, write queries against the cleaned DataFrame from Part 1.
# The table is already registered for you as "mission_events".
#
# Q1 - Joins & Aggregations:
#   For each study, show the total number of completed missions and
#   the average duration. Only include studies with more than 2 completions.
#
# Q2 - Window Functions:
#   For each participant, rank their missions by duration within each study,
#   longest first. Return participant_id, study_id, mission_id, duration_seconds,
#   and their rank.
#
# Q3 - Anomaly Detection:
#   Flag participants whose average mission duration is more than 2 standard
#   deviations from the mean duration for that study. Return the participant,
#   study, their average duration, the study mean, the study stddev, and
#   how many standard deviations away they are.

con = duckdb.connect()
con.register("mission_events", valid_events_df)

# Q1
q1 = """
-- YOUR QUERY HERE
"""

# Q2
q2 = """
-- YOUR QUERY HERE
"""

# Q3
q3 = """
-- YOUR QUERY HERE
"""

print("\n── Q1: Study Completion Summary ──")
print(con.execute(q1).df())

print("\n── Q2: Mission Rankings by Participant ──")
print(con.execute(q2).df())

print("\n── Q3: Anomalous Participants ──")
print(con.execute(q3).df())