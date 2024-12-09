from datetime import datetime
from pyflink.common import Row, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment, Schema, DataTypes

# setup DataStream API
env = StreamExecutionEnvironment.get_execution_environment()

# use BATCH or STREAMING mode
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

# setup Table API
table_env = StreamTableEnvironment.create(env)

# create a user stream
t_format = "%Y-%m-%dT%H:%M:%S"
user_stream = env.from_collection(
    [Row(datetime.strptime("2021-08-21T13:00:00", t_format), 1, "Alice"),
     Row(datetime.strptime("2021-08-21T13:05:00", t_format), 2, "Bob"),
     Row(datetime.strptime("2021-08-21T13:10:00", t_format), 2, "Bob")],
    type_info=Types.ROW_NAMED(["ts1", "uid", "name"],
                              [Types.SQL_TIMESTAMP(), Types.INT(), Types.STRING()]))

# create an order stream
order_stream = env.from_collection(
    [Row(datetime.strptime("2021-08-21T13:02:00", t_format), 1, 122),
     Row(datetime.strptime("2021-08-21T13:07:00", t_format), 2, 239),
     Row(datetime.strptime("2021-08-21T13:11:00", t_format), 2, 999)],
    type_info=Types.ROW_NAMED(["ts1", "uid", "amount"],
                        [Types.SQL_TIMESTAMP(), Types.INT(), Types.INT()]))

# # create corresponding tables
table_env.create_temporary_view(
    "user_table",
    user_stream,
    Schema.new_builder()
        .column_by_expression("ts", "CAST(ts1 AS TIMESTAMP(3))")
        .column("uid", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build())

table_env.create_temporary_view(
    "order_table",
    order_stream,
    Schema.new_builder()
        .column_by_expression("ts", "CAST(ts1 AS TIMESTAMP(3))")
        .column("uid", DataTypes.INT())
        .column("amount", DataTypes.INT())
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build())

# perform interval join
joined_table = table_env.sql_query(
    "SELECT U.name, O.amount " +
    "FROM user_table U, order_table O " +
    "WHERE U.uid = O.uid AND O.ts BETWEEN U.ts AND U.ts + INTERVAL '5' MINUTES")

joined_stream = table_env.to_data_stream(joined_table)

joined_stream.print()

# implement a custom operator using ProcessFunction and value state
class MyProcessFunction(KeyedProcessFunction):

    def __init__(self):
        self.seen = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("seen", Types.STRING())
        self.seen = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx):
        name = value[0]
        if self.seen.value() is None:
            self.seen.update(name)
            yield name

joined_stream \
    .key_by(lambda r: r[0]) \
    .process(MyProcessFunction()) \
    .print()

# execute unified pipeline
env.execute()

# prints (in both BATCH and STREAMING mode):
# +I[Bob, 239]
# +I[Alice, 122]
# +I[Bob, 999]
#
# Bob
# Alice