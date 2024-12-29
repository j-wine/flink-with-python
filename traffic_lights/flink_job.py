import json
from datetime import datetime

from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

env = StreamExecutionEnvironment.get_execution_environment()

env.add_jars("file:///taskscripts/jars/flink-connector-kafka-3.0.1-1.18.jar")
env.add_jars("file:///taskscripts/jars/kafka-clients-3.0.1.jar")
env.add_jars("file:///taskscripts/jars/flink-json-1.18.0.jar")


# Define the input schema for Kafka messages
input_schema = Types.ROW_NAMED(
    ["traffic_light_id", "primary_signal", "secondary_signal", "location", "timestamp"],
    [Types.STRING, Types.INT, Types.INT, Types.STRING, Types.STRING]
)

# Define the output schema for processed messages
output_schema = Types.ROW_NAMED(
    ["traffic_light_id", "primary_signal", "secondary_signal", "location", "signal_duration", "event_timestamp"],
    [Types.STRING, Types.INT, Types.INT, Types.STRING, Types.DOUBLE, Types.STRING]
)

# Define Kafka source
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("broker:9092") \
    .set_topics("traffic_light_signals") \
    .set_group_id("traffic-light-group") \
    .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(input_schema).build()) \
    .build()

# Define Kafka sink
kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("broker:9092") \
    .set_topic("processed_traffic_light_signals") \
    .set_record_serializer(JsonRowSerializationSchema.builder().with_type_info(output_schema).build()) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

# Cache for last signal timestamps
last_signal_timestamps = {}


class TransformSignal(MapFunction):
    """
    A MapFunction to transform incoming Kafka messages by adding `signal_duration`.
    """

    def map(self, value):
        global last_signal_timestamps
        # Parse the input JSON string
        data = json.loads(value)
        traffic_light_id = data["traffic_light_id"]
        current_timestamp = datetime.fromisoformat(data["timestamp"]).timestamp()

        # Calculate signal duration
        last_timestamp = last_signal_timestamps.get(traffic_light_id, current_timestamp)
        signal_duration = current_timestamp - last_timestamp
        last_signal_timestamps[traffic_light_id] = current_timestamp

        # Enrich the data with additional features
        data["signal_duration"] = signal_duration
        data["event_timestamp"] = datetime.utcfromtimestamp(current_timestamp).isoformat()

        # Return the transformed JSON as a string
        return json.dumps(data)


# Define the Flink job pipeline
kafka_stream = env.from_source(kafka_source, watermark_strategy=None, source_name="KafkaSource") \
    .map(TransformSignal(), output_type=SimpleStringSchema())

kafka_stream.sink_to(kafka_sink)

# Execute the Flink job
env.execute("Traffic Light Flink Job")
