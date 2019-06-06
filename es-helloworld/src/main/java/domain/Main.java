package domain;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zhangming
 * @date 2019/5/26 22:01
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws UnknownHostException {
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("localhost"), 9300);
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(transportAddress);

        SearchResponse response = client.prepareSearch("persons")
//                .addScriptField("code" , new Script("doc['_index']"))
//                .addAggregation(AggregationBuilders.count("_index").field("code"))
//                .setFetchSource(false).setExplain(false)
//                .setSize(0)
                .get();

        logger.info("response:[{}]", response);

    }
}
