import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchJavaUpdateAndDelete {
    private static RestHighLevelClient client = null;

    private static synchronized RestHighLevelClient makeConnection() {
        final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider
                .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "password"));

        if (client == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("localhost", 9200, "http"))
                            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                    httpClientBuilder.disableAuthCaching();
                                    return httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
                                }
                            })
            );
        }

        return client;
    }

    public static void main(String[] args) throws IOException {
        client = makeConnection();


        // Method 1: update a sample string value
        UpdateRequest updateRequest = new UpdateRequest("employees", "1");
        updateRequest.doc("age", 25);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("updated response id: "+updateResponse.getId());


        // Method 2: Update id with particular Map values
        Map<String, Object> updateMap = new HashMap<String, Object>();
        updateMap.put("id","3");
        updateMap.put("sex","male");
        updateMap.put("age", 40);
        updateMap.put("name", "liuxg");
        updateMap.put("addtionalfield", "anything");

        UpdateRequest request = new UpdateRequest("employees", "2").doc(updateMap);
        UpdateResponse updateResponse2= client.update(request, RequestOptions.DEFAULT);
        System.out.println("updated response id: "+updateResponse2.getId());


        // Method 3: Use index API to update
        IndexRequest request3 = new IndexRequest("employees");
        request3.id("1");
        request3.source("age", 20);
        IndexResponse indexResponse3 = client.index(request3, RequestOptions.DEFAULT);
        System.out.println("response id: " + indexResponse3.getId());
        System.out.println(indexResponse3.getResult().name());

        // Method 4: use index API to update via Map
        Map<String, Object> updateMap4 = new HashMap<String, Object>();
        updateMap4.put("field1","field1");
        updateMap4.put("field2","field2");
        updateMap4.put("field3", 30);
        IndexRequest request4 = new IndexRequest("employees");
        request4.id("2");
        request4.source(updateMap4);
        IndexResponse indexResponseUpdate4 = client.index(request4, RequestOptions.DEFAULT);
        System.out.println("response id: " + indexResponseUpdate4.getId());
        System.out.println(indexResponseUpdate4.getResult().name());

        // Method 5: Use a PoJo Java object
        Employee employee = new Employee("myid", "Martin");
        IndexRequest indexRequest5 = new IndexRequest("employees");
        indexRequest5.id("1");
        indexRequest5.source(new ObjectMapper().writeValueAsString(employee), XContentType.JSON);
        IndexResponse indexResponse5 = client.index(indexRequest5, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse5.getId());
        System.out.println("response name: "+indexResponse5.getResult().name());

        // Method 6: Update it with UpdateByQuery
        Map<String, Object> updateMap6 = new HashMap<String, Object>();
        updateMap6.put("id","66");
        updateMap6.put("name","Bill");
        UpdateByQueryRequest updateByQueryRequest6 = new UpdateByQueryRequest("employees");
        updateByQueryRequest6.setConflicts("proceed");
        updateByQueryRequest6.setQuery(new TermQueryBuilder("_id", "1"));
        Script script = new Script(ScriptType.INLINE, "painless","ctx._source = params", updateMap6);
        updateByQueryRequest6.setScript(script);

        try {
            BulkByScrollResponse bulkResponse = client.updateByQuery(updateByQueryRequest6, RequestOptions.DEFAULT);
            long totalDocs = bulkResponse.getTotal();
            System.out.println("updated response id: "+totalDocs);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Delete a document
        DeleteRequest deleteRequest = new DeleteRequest("employees","1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("response id: "+deleteResponse.getId());

        //Delete an index
        DeleteIndexRequest requestDeleteIndex = new DeleteIndexRequest("employees");
        client.indices().delete(requestDeleteIndex, RequestOptions.DEFAULT);
        System.out.println("Index is deleted ");
    }
}
