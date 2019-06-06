package es.common;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * 	索引 名词 		对应数据库
 *   	 动词 		分词，词条
 * </pre>
 *
 * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.0/transport-client.html">client
 * java api</a>
 * <p>
 * client会将每一个地址解析成一个elasticsearch节点，他会和每个地址之间形成很多connection，但是对于同一个地址的connection ，他们会连接到同一个节点。
 * <p>
 * 这就意味着你在配置client时候，需要添加上所有的对立节点，而不是使用一个负载均衡器将多个节点暴露在同一地址下。
 */
public class ElasticSearchIndexTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchIndexTest.class);

    private TransportClient client;

    /**
     * client.transport.sniff=true  允许动态添加节点删除节点
     * <p>
     * sniffing开启时候，client将会通过调用addTransportAddress与内部节点列表的上的节点进行连接
     * <p>
     * client 不会与没有数据的节点建立连接，避免主节点拥塞（因为连接的时候都会连接到主节点，如果不是这种机制，那么主节点将会有很多 connection）
     *
     * @throws UnknownHostException
     */
    @Before
    public void setUp() throws UnknownHostException {


//		Settings settings = Settings.builder().put("cluster.name", "myClusterName").put("client.transport.sniff", true).build();
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("localhost"), 9300);
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(transportAddress);
    }

    /**
     * 在 es 没有索引的情况下，插入文档，默认创建索引和索引映射，此时是无法用得到ik的（在ik已经配置的情况下） 我们需要手动创建索引
     * <p>
     * 通过 {@link XContentFactory#jsonBuilder()} 添加 doc
     * 此时
     *
     * @throws IOException
     */
    @Test
    public void testAddDoc() throws IOException {
        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("user", "kimchy")
                        .field("postDate", new Date()).field("message", "trying out Elasticsearch").endObject())
                .get();

        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();

        RestStatus status = response.status();
    }

    /**
     * 通过 json 字符串添加doc
     */
    @Test
    public void testAddDoc2() {
        String json = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\","
                + "\"message\":\"trying out Elasticsearch\"" + "}";

        // 可以添加id或者不添加 id
        IndexResponse response = client.prepareIndex("twitter", "tweet", "1").setSource(json, XContentType.JSON).get();
    }

    /**
     * 通过 map 转化为 byte[]
     */
    @Test
    public void testAddDoc3() throws JsonProcessingException {
        Map<String, Object> json = new HashMap<>();
        json.put("user", "kimchy");
        json.put("postDate", new Date());
        json.put("message", "trying out Elasticsearch");

        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = mapper.writeValueAsBytes(json);
    }

    /**
     * <pre>
     * boolQuery() 布尔查询 可以组合多个查询条件
     * fuzzyQuery() 相似度查询
     * matchAllQuery()  查询所有数据
     * regexQuery()  正则表达式查询
     * termQuery()  词条查询
     * wildcardQuery() 模糊查询
     * </pre>
     *
     * @throws UnknownHostException
     */
    @Test
    public void testSearch() throws UnknownHostException {
//			get() == execute().actionGet() 

        SearchResponse response = client.prepareSearch("blog1").setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery()).get();

        printResponse(response);

        client.close();
    }

    /**
     * queryStringQuery() 对内容进行分词查询 不区分字段
     *
     * @throws UnknownHostException
     */
    @Test
    public void testQueryStringQuery() throws UnknownHostException {

//			get() == execute().actionGet() 
        SearchResponse response = client.prepareSearch("blog1").setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("全文")).get();

        printResponse(response);
    }

    /**
     * 指定搜索字段 模糊查询 wildcardQuery() 也是基于词条查询的 termQuery() 词条查询 就是分词之后是否包含此term（或者
     * token）
     *
     * @throws UnknownHostException
     */
    @Test
    public void testWildcardQuery() throws UnknownHostException {
//			get() == execute().actionGet() 

        SearchResponse response = client.prepareSearch("blog1").setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*全文*")).get();

        printResponse(response);
    }

    private void printResponse(SearchResponse response) {
        SearchHits hits = response.getHits();
        System.out.println("查询结果有多少条：" + hits.getTotalHits());

        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }
    }

    /**
     * 创建索引（类似数据库） 不能重复创建
     */
    @Test
    public void testCreateIndex() {
        // 创建
        client.admin().indices().prepareCreate("blog2").get();
//		client.admin().indices().prepareDelete("blog2").get();

        client.close();
    }

    /**
     * 添加映射 主要是字段类型和分词器定义
     * <p>
     * ik 在es中配置
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void testCreateMapping() throws IOException, InterruptedException, ExecutionException {
//		 针对 type 
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties").startObject("id").field("type", "integer").field("store", "yes").endObject()
                .startObject("title").field("type", "string").field("store", "yes").endObject().startObject("content")
                .field("type", "string").field("store", "yes")
//				.field("analyzer", "ik")
                .endObject().endObject().endObject().endObject();

        PutMappingRequest request = Requests.putMappingRequest("blog2").type("article").source(builder);
        client.admin().indices().putMapping(request).get();
        client.close();
    }

}
