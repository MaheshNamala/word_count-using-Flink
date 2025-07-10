from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_batch_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

t_env.execute_sql("""
    CREATE TABLE input_table (
        word STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'input/stream_input.csv',
        'format' = 'csv'
    )
""")

t_env.execute_sql("""
    CREATE TABLE word_counts (
        word STRING,
        cnt BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

t_env.execute_sql("""
    INSERT INTO word_counts
    SELECT word, COUNT(*) AS cnt
    FROM input_table
    GROUP BY word
""")
