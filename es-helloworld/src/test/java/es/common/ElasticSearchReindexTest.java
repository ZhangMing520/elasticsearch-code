package es.common;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
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
public class ElasticSearchReindexTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchReindexTest.class);

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
     * 重建索引
     *
     * @throws UnknownHostException
     */
    @Test
    public void testReindex() throws UnknownHostException {
        BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client)
                .source("source_index")
                .destination("target_index")
                .filter(QueryBuilders.matchQuery("category", "xzy"))
                .get();

        logger.info("reindex response:[{}]", response);
    }

}
