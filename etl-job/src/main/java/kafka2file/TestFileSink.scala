//package kafka2file
//
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.SimpleStringEncoder
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//
////import org.apache.flink.api.common.functions.MapFunction
////import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
////import org.apache.flink.core.fs.Path
////import org.apache.flink.runtime.state.StateBackend
////import org.apache.flink.runtime.state.filesystem.FsStateBackend
////import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
////import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
////import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
////import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
////import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
//
//object TestFileSink {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    //checkpoint
//    //source
//    val props = new Properties
//    props.setProperty("bootstrap.servers", "localhost:9092")
////    val consumer = new FlinkKafkaConsumer010[String]("flink_orders", new SimpleStringSchema, props)
//
//    //transformation
////    val stream = env.addSource(consumer).map()
//
//    //        //sink
//    //        Encoder<String> myEncoder = new SimpleStringEncoder<>();
//    //        BucketAssigner<String, String> myBucketAssigner =  new EventTimeBucketAssigner();
//    //        BucketAssigner<String, String> myBucketAssigner =  new DateTimeBucketAssigner();
//
//    val sink = StreamingFileSink
//      .forRowFormat(new Path("/tmp/kafka-loader"), new SimpleStringEncoder[String])
//      .withRollingPolicy(DefaultRollingPolicy.builder().build())
//      .withBucketAssigner(new DateTimeBucketAssigner[String, String])
//      .build()
//
//    val sink1 = StreamingFileSink
//      .forRowFormat(new Path("/tmp/kafka-loader"), new SimpleStringEncoder)
//      .withRollingPolicy(DefaultRollingPolicy.builder().build())
//      .withBucketAssigner(new DateTimeBucketAssigner)
//      .build()
//
////    stream.addSink(sink)
//
//    env.execute
//  }
//}
