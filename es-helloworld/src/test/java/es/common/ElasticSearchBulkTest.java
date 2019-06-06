package es.common;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 */
public class ElasticSearchBulkTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBulkTest.class);

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
     * bulk有错误时候，{@link DocWriteResponse} 为 null
     *
     * bulk可以index，delete，update，get等操作
     *
     * @throws IOException
     */
    @Test
    public void testBulk() throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        bulkRequestBuilder.add(client.prepareIndex("twitter", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );

        bulkRequestBuilder.add(
                client.prepareIndex("twitter", "tweet", "2")
                        .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("user", "kimchy")
                                .field("postDate", new Date())
                                .field("message", "another post")
                                .endObject()
                        )
        );

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            BulkItemResponse[] itemResponses = bulkResponse.getItems();
            for (BulkItemResponse itemResponse : itemResponses) {
                DocWriteResponse response = itemResponse.getResponse();
                BulkItemResponse.Failure failure = itemResponse.getFailure();

                logger.info("response:[{}]", response);
                logger.info("failure:[{}]", failure);
            }
        }
    }
}
