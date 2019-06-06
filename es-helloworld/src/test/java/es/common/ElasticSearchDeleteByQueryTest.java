package es.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 */
public class ElasticSearchDeleteByQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDeleteByQueryTest.class);

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

    /**
     * filter  符合条件的doc会被删除
     *
     * @throws UnknownHostException
     */
    @Test
    public void testDeleteByQuery() throws UnknownHostException {
        BulkByScrollResponse scrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("gender", "male"))
                // index
                .source("persons")
                .get();

        // deleted number
        long deleted = scrollResponse.getDeleted();

        logger.info("[{}]", deleted);
        logger.info("deleted response:[{}]", scrollResponse);
    }

    /**
     * 主线程需要等待
     *
     * @throws InterruptedException
     */
    @Test
    public void testDeleteByQuery2() throws InterruptedException {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("gender", "male"))
                .source("persons")
                .execute(new ActionListener<BulkByScrollResponse>() {

                    @Override
                    public void onResponse(BulkByScrollResponse scrollResponse) {
                        long deleted = scrollResponse.getDeleted();
                        logger.info("deleted number:[{}]", deleted);
                        logger.info("deleted response:[{}]", scrollResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("delete failed.[{}]", e);
                    }
                });

        Thread.sleep(1000);
    }

}
