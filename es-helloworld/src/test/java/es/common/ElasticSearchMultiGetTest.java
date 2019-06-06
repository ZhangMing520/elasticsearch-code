package es.common;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
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
public class ElasticSearchMultiGetTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMultiGetTest.class);

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
     * index不存在时候，并不会报错，但是response是null
     * doc不存在时候，response 不是 null，可以通过 {@link GetResponse#isExists()} 过滤
     *
     * @throws UnknownHostException
     */
    @Test
    public void testMultiGet() throws UnknownHostException {
        MultiGetResponse multiGetResponse = client.prepareMultiGet().add("persons", "doc", "1")
                .add("persons", "doc", "2", "3")
                .add("another_index", "doc", "2", "3")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetResponse) {
            GetResponse response = itemResponse.getResponse();
            if (response != null && response.isExists()) {
                String json = response.getSourceAsString();
                logger.info("response source:[{}]", json);
            }
        }

    }

}
