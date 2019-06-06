package es.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
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
public class ElasticSearchTerminateAfterTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchTerminateAfterTest.class);

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
     * {@code  setTerminateAfter} 每个分片最大返回的文档数目。如果还有文档未获取，那么提前结束标志位设置为 true
     *
     * @throws UnknownHostException
     */
    @Test
    public void testTerminateAfter() throws UnknownHostException {
        SearchResponse response = client.prepareSearch("index").setTerminateAfter(1000)
                .get();

        if (response.isTerminatedEarly()) {
            // 是否提前结束
        }

        logger.info("search response:[{}]", response);
    }

}
