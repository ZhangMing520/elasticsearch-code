package es.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
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
public class ElasticSearchSearchTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSearchTest.class);

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
    public void testSearch() throws UnknownHostException {
        SearchResponse response = client.prepareSearch("index1", "index2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                query
                .setQuery(QueryBuilders.termQuery("multi", "test"))
//                filter
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))
                .setFrom(0).setSize(60).setExplain(true).get();

        logger.info("search response:[{}]", response);
    }

}
