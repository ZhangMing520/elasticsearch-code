package es.common;

import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 */
public class ElasticSearchBulkProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBulkProcessorTest.class);

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
     * bulkProcessor可以设置每次批量操作的数量，周期
     */
    @Test
    public void testBulkProcessor() throws InterruptedException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

            /**
             *每次批量开始执行
             *
             * @param executionId
             * @param request
             */
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.info("number of actions:{[]}", numberOfActions);
            }

            /**
             * 每次批量执行完成
             *
             * @param executionId
             * @param request
             * @param response
             */
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                boolean failures = response.hasFailures();
                logger.info("bulk has failures:[{}]", failures);
            }

            /**
             * 每次批量抛出异常时候执行
             *
             * @param executionId
             * @param request
             * @param failure
             */
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        })
                // 每1000个请求操作执行批量
                .setBulkActions(1000)
//             每5mb请求大小执行批量操作
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
//                每5秒执行批量大小，不管多少请求
                .setFlushInterval(TimeValue.timeValueSeconds(5))
//                0 一个线程执行，同步，1 在累积批量请求时候，可以有另一个线程执行
                .setConcurrentRequests(1)

//                重试机制，重试3次，每次等待100ms之后
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();


//        在 setConcurrentRequests(1) 情况下，等待关闭（超时时间内）
        boolean close = bulkProcessor.awaitClose(10, TimeUnit.MINUTES);


//      在 setConcurrentRequests(1) 情况下 不等待剩余的request执行完成
        bulkProcessor.close();

    }


    @Test
    public void testUseBulkProcessorInTests() {
        BulkProcessor processor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        })
                .setBulkActions(1000)
//                同步执行
                .setConcurrentRequests(0)
                .build();


        processor.add(new IndexRequest());

//        执行剩下的 request
        processor.flush();

        processor.close();

//        刷新index
        client.admin().indices().prepareRefresh().get();

        client.prepareSearch().get();
    }

}
