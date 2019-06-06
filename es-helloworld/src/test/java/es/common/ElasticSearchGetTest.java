package es.common;

import org.elasticsearch.action.get.GetResponse;
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
public class ElasticSearchGetTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchGetTest.class);

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

    @Test
    public void testGet() throws UnknownHostException {
        GetResponse response = client.prepareGet("twitter", "_doc", "1")
                .get();
        logger.info("[{}]", response);
    }

}
