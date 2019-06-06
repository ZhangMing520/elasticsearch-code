package es.admin;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
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
public class ElasticSearchClusterAdminTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClusterAdminTest.class);

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
    public void testClusterHealth() {
        ClusterHealthResponse response = client.admin().cluster().prepareHealth().get();

        String clusterName = response.getClusterName();
//        data nodes
        int numberOfDataNodes = response.getNumberOfDataNodes();
//        nodes
        int numberOfNodes = response.getNumberOfNodes();

        for (ClusterIndexHealth health : response.getIndices().values()) {
            String index = health.getIndex();
            int numberOfShards = health.getNumberOfShards();
            int numberOfReplicas = health.getNumberOfReplicas();

            ClusterHealthStatus status = health.getStatus();
        }
    }

    /**
     * 等待集群到特定的状态，然后获取 health 信息
     */
    @Test
    public void testWaitForStatus() {
        ClusterHealthResponse response = client.admin().cluster().prepareHealth()
                .setWaitForYellowStatus().get();

        // 没有等待到特定状态，那么手动报错
        ClusterHealthStatus status = response.getIndices().get("company").getStatus();
        if (status != ClusterHealthStatus.GREEN) {
            throw new RuntimeException("Index is in " + status + " state");
        }

        client.admin().cluster().prepareHealth()
                .setWaitForGreenStatus().get();

        client.admin().cluster().prepareHealth("employee")
                .setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(2))
                .get();
    }


    @Test
    public void testStoredScript() {
        // 保存脚本
        client.admin().cluster().preparePutStoredScript()
                .setId("script1")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"painless\", \"source\": \"_score * doc['my_numeric_field'].value\"} }"), XContentType.JSON);


//        获取脚本
        client.admin().cluster().prepareGetStoredScript()
                .setId("script1").get();

//        删除脚本
        client.admin().cluster().prepareDeleteStoredScript()
                .setId("script1").get();
    }
}
