import argparse
import logging
import sys
import json

from pyflink.common import WatermarkStrategy, Encoder, Row, Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention


def kafka_source(input_path, output_path):
    config = Configuration()
    # config.set_string("execution.checkpointing.storage","filesystem")
    # config.set_string("execution.checkpointing.dir","s3a://flink/pyflink-checkpoint")
    # config.set_string("s3.endpoint","http://minio:9000")
    # config.set_string("s3.access-key","admin")
    # config.set_string("s3.secret-key","password")
    # config.set_string("s3.path.style.access","true")
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.add_jars(
        "file:///opt/flink/opt/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///opt/flink/opt/kafka-clients-3.9.0.jar"
        )
    # write all the data to one file
    env.set_parallelism(1)

    # setup checkpoint
    # -- start a checkpoint every 10,000 ms
    env.enable_checkpointing(10000)
    #-- set mode exactly-one (this is the default)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    #-- make sure 5000 ms of progress happen between checkpoints
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    #-- checkpoints have to complete within one minute, or are discarded
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    #-- only two consecutive checkpoint failures are tolerated
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
    #-- allow only one checkpoint to be in progress at the same time
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    #-- enable externalized checkpoints which are retained after job cancellation
    env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
    #-- enables the unaligned checkpoints
    env.get_checkpoint_config().enable_unaligned_checkpoints()

    # kafka server source
    if input_path is not None:
        kafka_server = input_path
    else:
        kafka_server = "redpanda:9092"
    
    # kafka topic target
    if output_path is not None:
        topic_target = output_path
    else:
        topic_target = "transformed_orders"

    # read kafka source
    source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_server) \
            .set_topics("orders") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    
    # transform function
    def replace(message):
        data = json.loads(message)
        data["content"] = "transformed"
        return data

    ds = ds.map(replace)
    # ds.print()

    # convert data to row type
    row_type_info = Types.ROW_NAMED(field_names=["id","content"],field_types=[Types.INT(),Types.STRING()])
    json_format = JsonRowSerializationSchema.builder().with_type_info(row_type_info).build()
    ds = ds.map(lambda e: Row(id=e['id'], content=e['content']), output_type=row_type_info)
    # ds.print()

    # define the sink
    sink = KafkaSink.builder() \
            .set_bootstrap_servers(kafka_server) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(topic_target)
                    .set_value_serialization_schema(json_format)
                    .build()
            ) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

    ds.sink_to(sink)

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Kafka server address.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Topic of Kafka Sink.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    kafka_source(known_args.input, known_args.output)