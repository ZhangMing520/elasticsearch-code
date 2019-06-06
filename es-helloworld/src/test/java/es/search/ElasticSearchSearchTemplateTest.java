package es.search;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangming
 * @date 2019/5/26 21:01
 */
public class ElasticSearchSearchTemplateTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSearchTemplateTest.class);

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
     * 通过 {@code config/scripts } 保存搜索模板
     * <p>
     * 本例子对应的模板名称 {@code config/scripts/template_gender.mustache}
     * <pre>
     *     {
     *     "query" : {
     *         "match" : {
     *             "gender" : "{{param_gender}}"
     *         }
     *     }
     * }
     * </pre>
     *
     * @throws UnknownHostException
     */
    @Test
    public void testSearchTemplate() throws UnknownHostException {
//        定义模板参数
        Map<String, Object> template_params = new HashMap<>();
        template_params.put("param_gender", "male");


        SearchResponse response = new SearchTemplateRequestBuilder(client)
//                存储的模板名称
                .setScript("template_gender")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(template_params)
//                设置执行上下文  可以在此定义 index
                .setRequest(new SearchRequest())
                .get().getResponse();

        logger.info("search response:[{}]", response);
    }


    @Test
    public void testSearchTemplate2() {
//        定义模板参数
        Map<String, Object> template_params = new HashMap<>();
        template_params.put("param_gender", "male");

        SearchResponse response = new SearchTemplateRequestBuilder(client)
                .setScript("{\n" +
                        "        \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "                \"gender\" : \"{{param_gender}}\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "}")
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get().getResponse();

        logger.info("search response:[{}]", response);

    }


    @Test
    public void testSaveSearchTemplate() throws IOException {
        PutStoredScriptResponse response = client.admin().cluster().preparePutStoredScript()
                .setId("template_gender")
                .setContent(new BytesArray("{\n" +
                        "    \"query\" : {\n" +
                        "        \"match\" : {\n" +
                        "            \"gender\" : \"{{param_gender}}\"\n" +
                        "        }\n" +
                        "    }\n" +
                        "}"), XContentFactory.jsonBuilder().contentType()).get();
    }

}
