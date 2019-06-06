package es.search;

import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Response;
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
public class ElasticSearchMultiSearchTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMultiSearchTest.class);

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
    public void testMultiSearch() throws UnknownHostException {

        SearchRequestBuilder searchRequestBuilder1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch"))
                .setSize(1);

        SearchRequestBuilder searchRequestBuilder2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

        MultiSearchResponse multiSearchResponse = client.prepareMultiSearch().add(searchRequestBuilder1)
                .add(searchRequestBuilder2).get();


        // 获取每个响应
        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
        long nHits = 0;
        for (MultiSearchResponse.Item item : responses) {
            SearchResponse response = item.getResponse();
            nHits += response.getHits().getTotalHits();

            logger.info("search response:[{}]", response);
        }
    }

}
