"""
Dscout - AI Data Engineering Interview Problem
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

import math
from collections import Counter

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
# SELF-CHECK HELPERS
# ─────────────────────────────────────────────
# Lightweight print-based checks (no unittest/pytest) so you can see whether
# your implementation lines up with the known-correct output for this fixed
# dataset. They check *outcomes* - which records ended up where, and what the
# query results are - not implementation details like exact rejection_reason
# wording or column names/order, since those aren't prescribed above.

def _check(label: str, passed: bool, detail: str = "") -> bool:
    mark = "✓" if passed else "✗"
    print(f"  {mark} {label}" + (f" — {detail}" if detail and not passed else ""))
    return passed


def _rows_match(df, expected_rows, tol: float = 0.01) -> bool:
    """Compare a DataFrame's rows (any column names/order) against a list of
    expected tuples, ignoring row order and using a tolerance for numeric
    columns (e.g. averages)."""
    if len(df) != len(expected_rows):
        return False

    def _tuples_match(a, b):
        if len(a) != len(b):
            return False
        for x, y in zip(a, b):
            try:
                if not math.isclose(float(x), float(y), abs_tol=tol):
                    return False
            except (TypeError, ValueError):
                if x != y:
                    return False
        return True

    sort_key = lambda t: tuple(str(v) for v in t)
    actual = sorted((tuple(r) for r in df.itertuples(index=False, name=None)), key=sort_key)
    expected = sorted(expected_rows, key=sort_key)
    return all(_tuples_match(a, b) for a, b in zip(actual, expected))


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
# SELF-CHECK: PART 1
# ─────────────────────────────────────────────
# Known-correct outcomes for the fixed raw_events above:
#   - abc123 appears twice -> first copy valid, second copy rejected as a duplicate
#   - def456 is missing participant_id -> rejected
#   - ghi789 has a malformed timestamp and a negative duration -> rejected
#   - everything else is clean -> valid
# Checked by event_id (as a Counter, so the two abc123 copies aren't conflated)
# rather than exact rejection_reason text, since that wording is up to you.

print("\n── Self-check: Part 1 ──")

_expected_valid_ids = Counter(["abc123", "jkl012", "mno345", "pqr678", "stu901", "vwx234"])
_expected_rejected_ids = Counter(["abc123", "def456", "ghi789"])
_actual_valid_ids = Counter(r.get("event_id") for r in valid_events)
_actual_rejected_ids = Counter(r.get("event_id") for r in rejected_events)

_p1_checks = [
    _check(
        "6 valid / 3 rejected",
        len(valid_events) == 6 and len(rejected_events) == 3,
        f"got {len(valid_events)} valid / {len(rejected_events)} rejected",
    ),
    _check(
        "valid event_ids are exactly the expected ones",
        _actual_valid_ids == _expected_valid_ids,
        f"got {dict(_actual_valid_ids)}",
    ),
    _check(
        "rejected event_ids are exactly the expected ones (abc123 = the duplicate copy)",
        _actual_rejected_ids == _expected_rejected_ids,
        f"got {dict(_actual_rejected_ids)}",
    ),
    _check(
        "every rejected record has a non-empty rejection_reason",
        all(r.get("rejection_reason") for r in rejected_events),
    ),
]
print("All Part 1 checks passed!" if all(_p1_checks) else "Some Part 1 checks failed — see ✗ above.")


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

q1_df = con.execute(q1).df()
print("\n── Q1: Study Completion Summary ──")
print(q1_df)

q2_df = con.execute(q2).df()
print("\n── Q2: Mission Rankings by Participant ──")
print(q2_df)

q3_df = con.execute(q3).df()
print("\n── Q3: Anomalous Participants ──")
print(q3_df)

# ─────────────────────────────────────────────
# SELF-CHECK: PART 2
# ─────────────────────────────────────────────
# Known-correct results for each query, assuming Part 1 produced the expected
# clean dataset above. Column order is assumed to match what's described in
# the prompt for each query; your column names can be whatever you chose.

print("\n── Self-check: Part 2 ──")

_expected_q1 = [("s1", 3, 137.33), ("s2", 3, 456.67)]  # study_id, completed_missions, avg_duration_seconds
_q1_ok = _check(
    "Q1 matches expected study summary (s1: 3 @ ~137.3s, s2: 3 @ ~456.7s)",
    _rows_match(q1_df, _expected_q1),
)

_expected_q2 = [  # participant_id, study_id, mission_id, duration_seconds, duration_rank
    ("p1", "s1", "m1", 142, 1),
    ("p1", "s1", "m2", 95, 2),
    ("p3", "s2", "m3", 300, 1),
    ("p4", "s2", "m3", 210, 1),
    ("p5", "s1", "m2", 175, 1),
    ("p5", "s2", "m3", 860, 1),
]
_q2_ok = _check(
    "Q2 matches expected rankings (only p1 has >1 mission in a study)",
    _rows_match(q2_df, _expected_q2),
)

_q3_ok = _check(
    "Q3 is empty (too few points per study/participant to clear the 2-stddev bar)",
    len(q3_df) == 0,
)

print("All Part 2 checks passed!" if _q1_ok and _q2_ok and _q3_ok else "Some Part 2 checks failed — see ✗ above.")
