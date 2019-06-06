package es.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
public class ElasticSearchScrollSearchTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchScrollSearchTest.class);

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
     * {@code scrollResp.getHits().getHits().length != 0}  scroll 结束
     * <p>
     * scrollResp 是两层结构，{@code  scrollResp.getHits().getHits()} 才是文档内容
     *
     * @throws UnknownHostException
     */
    @Test
    public void testScrollSearch() throws UnknownHostException {

        SearchResponse response = client.prepareSearch("test")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.termQuery("multi", "test"))
//                每次 scroll 返回100条数据
                .setSize(1000)
                .get();

        do {
//            两层结果
            for (SearchHit hit : response.getHits().getHits()) {
                // handle hit
            }
        } while (response.getHits().getHits().length != 0); // end of the scroll

        logger.info("search response:[{}]", response);
    }

}
