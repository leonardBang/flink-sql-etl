package kafka2file;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class StreamETLKafka2Hdfs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //checkpoint
        env.enableCheckpointing(60_000);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///tmp/flink/checkpoints"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(
                "flink_orders", new SimpleStringSchema(), props);

        //transformation
        DataStream<String> stream = env.addSource(consumer)
                .map(r -> r);

        //sink
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("/tmp/kafka-loader"), new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .build();
        stream.addSink(sink);

        env.execute();
    }
}

