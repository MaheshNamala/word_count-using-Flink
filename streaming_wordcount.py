"""
tumbling_window_wordcount.py
-----------------------------
Real-time word count with event-time tumbling windows using PyFlink.

This script uses timestamped input from a socket, assigns watermarks,
and performs a tumbling window aggregation (10-second interval).

Run Requirements:
- Python 3.10+
- pip install pyflink
- nc -lk 9999 (with input like "hello,2024-07-10 10:12:15")

"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# 1. Setup stream environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# 2. Define input with timestamp + watermark (event-time)
t_env.execute_sql("""
    CREATE TABLE input_table (
        word STRING,
        ts TIMESTAMP(3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'socket',
        'hostname' = 'localhost',
        'port' = '9999',
        'format' = 'csv'
    )
""")

# 3. Define print output sink
t_env.execute_sql("""
    CREATE TABLE word_counts (
        word STRING,
        cnt BIGINT,
        window_end TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
""")

# 4. Perform tumbling window aggregation (10s)
t_env.execute_sql("""
    INSERT INTO word_counts
    SELECT word, COUNT(*) AS cnt, TUMBLE_END(ts, INTERVAL '10' SECOND) AS window_end
    FROM input_table
    GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), word
""")
