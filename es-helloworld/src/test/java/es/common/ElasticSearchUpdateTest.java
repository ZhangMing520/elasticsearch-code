package es.common;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 * <p>
 * update 操作支持部分更新以及新增字段（不存在就添加此字段）
 */
public class ElasticSearchUpdateTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUpdateTest.class);

    private TransportClient client;

    private final String INDEX_NAME = "persons";

    @Before
    public void setUp() throws IOException {
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("localhost"), 9300);
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(transportAddress);

//        for (int i = 1; i < 4; i++) {
//            Map<String, Object> map = new HashMap<>();
//            map.put("gender", i % 2 == 0 ? "male" : "female");
//
//            IndexResponse response = client.prepareIndex(INDEX_NAME, "doc", i + "")
//                    .setSource(map).get();
//            logger.info("index doc:[{}]", response);
//        }
    }

    @After
    public void tearDown() {
        client.close();
    }

    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("persons").type("doc").id("1")
                .doc(XContentFactory.jsonBuilder().startObject()
                        .field("gender", "male").endObject());
        UpdateResponse updateResponse = client.update(updateRequest).actionGet();

        logger.info("update response:[{}]", updateResponse);
    }


    @Test
    public void testUpdate2() throws IOException {
        UpdateResponse response = client.prepareUpdate("persons", "doc", "1")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("gender", "female").endObject()).get();

        logger.info("update response:[{}]", response);
    }

    @Test
    public void testUpdate3() {
        UpdateResponse response = client.prepareUpdate("persons", "doc", "1")
                .setScript(new Script("ctx._source.age='18'")).get();

        logger.info("update response:[{}]", response);
    }

    /**
     * 如果doc不存在，那么indexRequest中执行，否则执行updateRequest内容
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testUpsert() throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME, "doc", "1")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("name", "joe smith")
                        .field("gender", "male")
                        .endObject());


        UpdateRequest updateRequest = new UpdateRequest(INDEX_NAME, "doc", "1")
                .doc(XContentFactory.jsonBuilder().startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);

        UpdateResponse response = client.update(updateRequest).get();
        logger.info("update response:[{}]", response);
    }

}
