from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table import DataTypes
from pyflink.table.udf import udtf
from pyflink.table.expressions import col, call

# 1. Set up the PyFlink environment in batch mode
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_batch_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# 2. Define input table (from CSV file)
t_env.execute_sql("""
    CREATE TABLE input_table (
        line STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'input/sample.txt',
        'format' = 'csv'
    )
""")

# 3. Define a Python UDTF (table function) using @udtf
@udtf(result_types=[DataTypes.STRING()])
def split_words(line: str):
    for word in line.split():
        yield word,

# 4. Register the UDTF
t_env.create_temporary_function("split_words", split_words)

# 5. Read the table
table = t_env.from_path("input_table")

# 6. Use expression-based join_lateral (✅ fixed)
words_table = table \
    .join_lateral(call("split_words", col("line")).alias("word")) \
    .group_by(col("word")) \
    .select(col("word"), col("word").count.alias("cnt"))

# 7. Define the output table (print)
t_env.execute_sql("""
    CREATE TABLE result_table (
        word STRING,
        cnt BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

# 8. Insert the result into the print sink
words_table.execute_insert("result_table").wait()
