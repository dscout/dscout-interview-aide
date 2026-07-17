# Dscout — Data Engineering Interview: Round 1

Welcome! This coding exercise covers a Python data pipeline and SQL. It's meant to reflect the kind of work you'd actually do here: ingesting messy event data, cleaning it, and answering analytical questions against it.

## Setup

You'll need Python 3.9+ and two packages:

```bash
pip install duckdb pandas
```

## The file

Everything you need is in `ai_data_eng_tech_round.py`:

- Sample raw event data (with some intentionally messy records mixed in)
- Part 1: a `process_events()` function stub for you to implement
- Part 2: three SQL query stubs to fill in, run against your cleaned data via DuckDB

Read the docstring and inline instructions in the file. They spell out the requirements for each part.

## Working through it

1. Start with Part 1. Implement `process_events()` so it deduplicates, validates, and separates the raw events into `valid_events` and `rejected_events`.
2. Once that produced sensible output, move to Part 2 and write the three SQL queries against the cleaned data.
3. You can run the file at any point to check your progress:

   ```bash
   python3 data_eng_tech_round.py
   ```

   It will print your valid/rejected event counts and (once you've written them) the results of each SQL query.

## What we're looking for

There's no single "correct" implementation! We're interested in how you think about:

- Data validation and what counts as a clean vs. rejected record
- Making pipeline failures visible and debuggable (not just silently dropped)
- Writing SQL that's correct on edge cases, not just the happy path
- How you'd test this code

## Submitting

Please send back your completed `ai_data_eng_tech_round.py` file along with any notes on tradeoffs or assumptions you made.
