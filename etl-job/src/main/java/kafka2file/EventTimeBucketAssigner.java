package kafka2file;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EventTimeBucketAssigner implements BucketAssigner<String, String> {
    private ObjectMapper mapper = new ObjectMapper();
    @Override
    public String getBucketId(String element, Context context) {
        String partitionValue;
        try {
            JsonNode node = mapper.readTree(element);
            long date = (long) (node.path("order_time").floatValue() * 1000);
            partitionValue = new SimpleDateFormat("yyyyMMdd").format(new Date(date));
        } catch (Exception e){
            partitionValue = "00000000";
        }
        return "dt=" + partitionValue;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}