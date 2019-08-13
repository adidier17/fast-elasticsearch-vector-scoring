package com.liorkn.elasticsearch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Lior Knaany on 4/7/18.
 */
public class PluginTestAttention {

    private static EmbeddedElasticsearchServer esServer;
    private static RestClient esClient;

    @BeforeClass
    public static void init() throws Exception {
        esServer = new EmbeddedElasticsearchServer();
        esClient = RestClient.builder(new HttpHost("localhost", esServer.getPort(), "http")).build();

        // delete test index if exists
        try {
            esClient.performRequest("DELETE", "/test", Collections.emptyMap());
        } catch (Exception e) {}

        // create test index
        String mappingJson = "{\n" +
                " \"settings\": {\n" +
                "   \"number_of_shards\": 1" +
                " }, \n" +
                "  \"mappings\": {\n" +
                "    \"type\": {\n" +
                "      \"properties\": {\n" +
                "        \"embedding_vector\": {\n" +
                "          \"doc_values\": true,\n" +
                "          \"type\": \"binary\"\n" +
                "        },\n" +
                "        \"job_id\": {\n" +
                "          \"type\": \"long\"\n" +
                "        },\n" +
                "        \"vector\": {\n" +
                "          \"type\": \"float\"\n" +
                "        },\n" +
                "        \"rows\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"cols\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        esClient.performRequest("PUT", "/test", Collections.emptyMap(), new NStringEntity(mappingJson, ContentType.APPLICATION_JSON));
    }

    public static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }


    @Test
    public void test() throws Exception {
        final Map<String, String> params = new HashMap<>();
        params.put("refresh", "true");
        final TestObject2D[] objs = {new TestObject2D(1, new float[][] {{0.0f, 0.5f, 1.0f},{1.6f, 7.3f, 0.2f}}, 2, 3),
                new TestObject2D(2, new float[][] {{0.2f, 0.6f, 0.99f}, {1.4f, 3.2f, 0.7f}}, 2, 3)};

        for (int i = 0; i < objs.length; i++) {
            final TestObject2D t = objs[i];
            final String json = mapper.writeValueAsString(t);
            System.out.println(json);
            final Response put = esClient.performRequest("PUT", "/test/type/" + t.jobId, params, new StringEntity(json, ContentType.APPLICATION_JSON));
            System.out.println(put);
            System.out.println(EntityUtils.toString(put.getEntity()));
            final int statusCode = put.getStatusLine().getStatusCode();
            Assert.assertTrue(statusCode == 200 || statusCode == 201);
        }

        float[] test1 = new float[] {0.1f, 0.2f, 0.3f};
        String test1_encoded = Util.convertArrayToBase64(test1);
        float[] decoded = Util.convertBase64ToArray(test1_encoded);
        System.out.println("decoded test " + decoded);
        float[][] test_vector = new float[][] {{0.1f, 0.2f, 0.3f}, {1.1f, 2.2f, 3.3f}};
        String test_emb_vector = Util.convert2dArrayToBase64(test_vector);
        System.out.println("encoded test vector "+test_emb_vector);
        // Test cosine score function
        String body = "{" +
                "  \"query\": {" +
                "    \"function_score\": {" +
                "      \"boost_mode\": \"replace\"," +
                "      \"script_score\": {" +
                "        \"script\": {" +
                "          \"source\": \"binary_vector_score\"," +
                "          \"lang\": \"knn\"," +
                "          \"params\": {" +
                "            \"cosine\": true," +
                "            \"field\": \"embedding_vector\"," +
                "            \"vector\":"  + "\"" + test_emb_vector + "\"," +
//                "            \"vector\":"  + "\"" + test_vector + "\"," +
                "            \"rows\": 2," +
                "            \"cols\": 3" +
                "          }" +
                "        }" +
                "      }" +
                "    }" +
                "  }," +
                "  \"size\": 100" +
                "}";
//        Object body2 = {  "query": {    "function_score": {      "boost_mode": "replace",      "script_score": {        "script": {          "source": "binary_vector_score",          "lang": "knn",          "params": {            "cosine": true,            "field": "embedding_vector",            "embedding_vector":"PczMzT5MzM0+mZmaP4zMzUAMzM1AUzMz",            "rows": 2,            "cols": 3          }        }      }    }  },  "size": 100}
        final Response res = esClient.performRequest("POST", "/test/_search", Collections.emptyMap(), new NStringEntity(body, ContentType.APPLICATION_JSON));
        System.out.println(res);
        final String resBody = EntityUtils.toString(res.getEntity());
        System.out.println(resBody);
        Assert.assertEquals("search should return status code 200", 200, res.getStatusLine().getStatusCode());
        Assert.assertTrue(String.format("There should be %d documents in the search response", objs.length), resBody.contains("\"hits\":{\"total\":" + objs.length));
        // Testing Scores
        final ArrayNode hitsJson = (ArrayNode)mapper.readTree(resBody).get("hits").get("hits");
        Assert.assertEquals(0.9941734, hitsJson.get(0).get("_score").asDouble(), 0);
        Assert.assertEquals(0.95618284, hitsJson.get(1).get("_score").asDouble(), 0);
    }

    @AfterClass
    public static void shutdown() {
        try {
            esClient.close();
            esServer.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
