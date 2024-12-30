import json
import os
from datetime import datetime
from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

env = StreamExecutionEnvironment.get_execution_environment()

# env.add_jars("file:///taskscripts/jars/flink-connector-kafka-3.0.1-1.18.jar")
# env.add_jars("file:///taskscripts/jars/kafka-clients-3.0.1.jar")
# env.add_jars("file:///taskscripts/jars/flink-connector-kafka-3.2.0-1.18.jar")
env.add_jars("file:///taskscripts/jars/kafka-clients-3.2.0.jar")
env.add_jars("flink-sql-connector-kafka-3.2.0-1.18.jar")

input_schema = Types.ROW_NAMED(
    ["traffic_light_id", "primary_signal", "secondary_signal", "location", "timestamp"],
    [Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING()]
)

output_schema = Types.ROW_NAMED(
    ["traffic_light_id", "primary_signal", "secondary_signal", "location", "signal_duration", "event_timestamp"],
    [Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.DOUBLE(), Types.STRING()]
)

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers(os.environ["KAFKA_BROKER"]) \
    .set_topics("traffic_light_signals") \
    .set_group_id("traffic-light-group") \
    .set_value_only_deserializer(JsonRowDeserializationSchema.builder()
                                 .type_info(input_schema).build()) \
    .build()

record_serializer = KafkaRecordSerializationSchema.builder() \
    .set_topic("processed_traffic_light_signals") \
    .set_value_serialization_schema(
    JsonRowSerializationSchema.builder()
    .with_type_info(output_schema)
    .build()
) \
    .build()

kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers(os.environ["KAFKA_BROKER"]) \
    .set_record_serializer(record_serializer) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

last_signal_timestamps = {}


class TransformSignal(MapFunction):
    def map(self, value):
        global last_signal_timestamps
        data = json.loads(value)
        traffic_light_id = data["traffic_light_id"]
        current_timestamp = datetime.fromisoformat(data["timestamp"]).timestamp()
        last_timestamp = last_signal_timestamps.get(traffic_light_id, current_timestamp)
        signal_duration = current_timestamp - last_timestamp
        last_signal_timestamps[traffic_light_id] = current_timestamp
        data["signal_duration"] = signal_duration
        data["event_timestamp"] = datetime.utcfromtimestamp(current_timestamp).isoformat()
        return json.dumps(data)


kafka_stream = (env.from_source(kafka_source,
                                watermark_strategy=WatermarkStrategy.no_watermarks(),
                                source_name="KafkaSource")
                .map(TransformSignal()))

kafka_stream.sink_to(kafka_sink)

env.execute("Traffic Light Flink Job")
