from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Table with timestamp
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

# Output to print
t_env.execute_sql("""
    CREATE TABLE word_counts (
        word STRING,
        cnt BIGINT,
        window_end TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
""")

# Count with 10-sec window
t_env.execute_sql("""
    INSERT INTO word_counts
    SELECT word, COUNT(*) AS cnt, TUMBLE_END(ts, INTERVAL '10' SECOND) AS window_end
    FROM input_table
    GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), word
""")
