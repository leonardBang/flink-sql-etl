package kafka2es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.auth.AuthScope;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.client.CredentialsProvider;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.RestClientBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Kafak2DynamicIndexEs {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        tableEnvironment.registerFunction("ts2Date", new Kafka2AppendEs.ts2Date());
        String csvSourceDDL = "create table csv(" +
                " user_name VARCHAR," +
                " is_new BOOLEAN," +
                " content VARCHAR" +
                ") with (" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '/Users/bang/sourcecode/project/flink-sql-etl/data-generator/src/main/resources/user.csv',\n" +
                " 'format.type' = 'csv',\n" +
                " 'format.fields.0.name' = 'user_name',\n" +
                " 'format.fields.0.data-type' = 'STRING',\n" +
                " 'format.fields.1.name' = 'is_new',\n" +
                " 'format.fields.1.data-type' = 'BOOLEAN',\n" +
                " 'format.fields.2.name' = 'content',\n" +
                " 'format.fields.2.data-type' = 'STRING')";
        tableEnvironment.sqlUpdate(csvSourceDDL);

        DataStream<Row> input = tableEnvironment.toAppendStream(
                tableEnvironment.sqlQuery("select user_name,is_new, content from csv "),
                Row.class);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

// use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Row>() {
                    public IndexRequest createIndexRequest(Row element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("user_name", element.getField(0).toString());
                        json.put("is_new", element.getField(1).toString());
                        json.put("content", element.getField(2).toString());

                        return Requests.indexRequest()
                                .index("test")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

// provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                            // elasticsearch username and password
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//                          credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("$USERNAME", "$PASSWORD"));
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
                }
        );

// finally, build and add the sink to the job's pipeline
        input.addSink(esSinkBuilder.build());

        env.execute("esDynamicIndex");

    }
}
