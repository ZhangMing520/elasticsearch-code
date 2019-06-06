package es.common;

import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 */
public class ElasticSearchUpdateByQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUpdateByQueryTest.class);

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
    public void testUpdateByQuery() {
        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .source("source_index")
//                版本冲突时候，也更新
                .abortOnVersionConflict(false)
                .get();

//       如果
        List<BulkItemResponse.Failure> failures = response.getBulkFailures();

        logger.info("update by query response:[{}]", response);
    }

    /**
     * 通过脚本更新
     */
    @Test
    public void testUpdateByQuery2() {
        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .source("source_index")
                .filter(QueryBuilders.termQuery("level", "awesome"))
//                限制更新文档数目
                .size(1000)
//                通过脚本更新
                .script(new Script(ScriptType.INLINE, "ctx._source.awesome='absolutely'"
                        , "painless", Collections.emptyMap()
                )).get();

        logger.info("update by query response:[{}]", response);
    }

    /**
     * 也可以通过 updateByQuery 获取文档，而不是更新
     */
    @Test
    public void testUpdateByQuery3() {
        SearchResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .source("source_index").source().setSize(500).addSort("cat", SortOrder.DESC).get();

        logger.info("update by query response:[{}]", response);
    }

    /**
     * 使用脚本更新 _source 字段，和Update类似
     * <p>
     * noop     什么都不做   noop计数器增加
     * delete  删除文档   deleted计数器增加
     */
    @Test
    public void testUpdateByQuery4() {
        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .source("source_index")
                .script(new Script(ScriptType.INLINE,
                        "if(ctx._source.awesome=='absolutely'){" +
                                "  ctx.op='noop'" +
                                "}else if(ctx._source.awesome=='lame'){" +
                                "  ctx.op='delete'" +
                                "}else {" +
                                " ctx._source.awesome='absolutely'" +
                                "}", "painless", Collections.emptyMap()))
                .get();

        logger.info("update by query response:[{}]", response);
    }

    /**
     * 在多个 index，多个type上执行
     */
    @Test
    public void testUpdateByQuery5() {
        SearchResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .source("foo", "bar").source().setTypes("a", "b")
                .get();

        logger.info("update by query response:[{}]", response);
    }

    /**
     * 获取正在运行的 updateByQuery 状态
     *
     * 或者取消正在运行的 updateByQuery
     */
    @Test
    public void testUpdateByQuery6() {
        ListTasksResponse listTasksResponse = client.admin().cluster().prepareListTasks()
                .setActions(UpdateByQueryAction.NAME).setDetailed(true).get();

        for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
            TaskId taskId = taskInfo.getTaskId();
            BulkByScrollTask.Status status = (BulkByScrollTask.Status) taskInfo.getStatus();

            // 获取任务详情
            GetTaskResponse response = client.admin().cluster().prepareGetTask(taskId).get();
            logger.info("update by query response:[{}]", response);

            // cancel one
            List<TaskInfo> tasks = client.admin().cluster().prepareCancelTasks().setTaskId(taskId).get().getTasks();


//            节流，限制每秒请求数目
            ListTasksResponse tasksResponse = RethrottleAction.INSTANCE.newRequestBuilder(client)
                    .setTaskId(taskId)
                    .setRequestsPerSecond(2.0f)
                    .get();
        }

//        cancel all
        CancelTasksResponse cancelTasksResponse = client.admin().cluster().prepareCancelTasks().setActions(UpdateByQueryAction.NAME).get();
    }



}
