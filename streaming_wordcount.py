from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Socket table
t_env.execute_sql("""
    CREATE TABLE input_table (
        word STRING
    ) WITH (
        'connector' = 'socket',
        'hostname' = 'localhost',
        'port' = '9999',
        'format' = 'raw'
    )
""")

# Output to console
t_env.execute_sql("""
    CREATE TABLE word_counts (
        word STRING,
        count BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

# Count logic
t_env.execute_sql("""
    INSERT INTO word_counts
    SELECT word, COUNT(*) as count
    FROM input_table
    GROUP BY word
""")
