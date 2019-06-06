package es.admin;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 */
public class ElasticSearchAdminTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchAdminTest.class);

    private TransportClient client;

    @Before
    public void setUp() throws UnknownHostException {
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("localhost"), 9300);
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(transportAddress);
    }

    @After
    public void tearDown() {
        client.close();
    }

    /**
     * 创建索引，也可以设置 settings
     *
     * @throws UnknownHostException
     */
    @Test
    public void testCreateIndex() throws UnknownHostException {
        CreateIndexResponse response = client.admin().indices().prepareCreate("index_name")
                // settings
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 1)
                )
                .get();
        logger.info("search response:[{}]", response);
    }


    /**
     * 创建 index 时候，设置 mapping
     */
    @Test
    public void testPutMapping() {
        CreateIndexResponse response = client.admin().indices().prepareCreate("index_name")
//                添加 mapping
                .addMapping("type_name", "message", "type=text")
                .get();

        logger.info("search response:[{}]", response);
    }

    /**
     * 创建 mapping 或者更新已有 mapping
     */
    @Test
    public void testPutMapping2() {
        client.admin().indices().preparePutMapping("index_name")
                .setType("type_name")
                .setSource("{\n" +
                        "  \"properties\": {\n" +
                        "    \"name\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}", XContentType.JSON).get();


//        source 中包含 type 名称
        client.admin().indices().preparePutMapping("index_name")
                .setType("type_name")
                .setSource("{\n" +
                        "    \"type_name\":{\n" +
                        "        \"properties\": {\n" +
                        "            \"name\": {\n" +
                        "                \"type\": \"text\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}", XContentType.JSON).get();
    }


    /**
     * 刷新 index
     */
    @Test
    public void testRefreshIndex() {
        client.admin().indices().prepareRefresh("twitter", "company").get();
    }


    /**
     * 获取 settings
     */
    @Test
    public void testGetSettings() {
        GetSettingsResponse response = client.admin().indices().prepareGetSettings("company", "employee").get();
        for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            String key = cursor.key;
            Settings settings = cursor.value;

            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
        }
    }

    /**
     * 更新 index settings
     */
    @Test
    public void updateIndexSettings() {
        client.admin().indices().prepareUpdateSettings("twitter")
                .setSettings(Settings.builder()
                        .put("index.number_of_replicas", 0)).get();
    }

}
