package es.common;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import domain.Article;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchDocTest {

	private TransportClient client;

	@Before
	public void setUp() throws UnknownHostException {
//		连接对象
		client = null ;
	}

	/**
	 * 从 json 添加文档，在开发过程中需要从 map list 中获取
	 * 
	 * @throws JsonProcessingException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void testAddDoc() throws JsonProcessingException, InterruptedException, ExecutionException {
		Article article = new Article();
		article.setId(2);
		article.setTitle("elasticsearch head 插件安装和使用");
		article.setContent(" elasticsearch  head 是集群管理工具、数据可视化、增删改查工具， Elasticsearch  语句可视化（下面会讲到）");

		ObjectMapper mapper = new ObjectMapper();

		client.prepareIndex("blog2", "article", article.getId().toString())
				.setSource(mapper.writeValueAsString(article)).get();

//		修改
		client.prepareUpdate("blog2", "article", article.getId().toString()).setDoc(mapper.writeValueAsString(article))
				.get();

//		直接update 
		client.update(new UpdateRequest("blog2", "article", article.getId().toString())
				.doc(mapper.writeValueAsString(article))).get();

//		prepareDelete   delete 删除

		client.close();
	}

	/**
	 * 批量插入
	 * 
	 * @throws JsonProcessingException
	 */
	@Test
	public void testBulikAddDoc() throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();

		BulkRequestBuilder bulk = client.prepareBulk();
		for (int i = 0; i < 100; i++) {
			Article article = new Article();
			article.setId(i);
			article.setTitle(i + "elasticsearch head 插件安装和使用");
			article.setContent(i + " elasticsearch  head 是集群管理工具、数据可视化、增删改查工具， Elasticsearch  语句可视化（下面会讲到）");

			bulk.add(new IndexRequest("blog2", "article", i + "").source(mapper.writeValueAsString(article)));
		}

		bulk.execute();
	}

	/**
	 * 分页查询
	 * 
	 */
	@Test
	public void testPageQuery() {
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog2").setTypes("article")
				.setQuery(QueryBuilders.matchAllQuery()).addSort("id", SortOrder.ASC);

		int from = 10;
		int size = 20;
		SearchResponse response = searchRequestBuilder.setFrom(from).setSize(size).get();

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
	 * 高亮标记
	 */
	@Test
	public void testHighlight() {
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog2").setTypes("article")
				.setQuery(QueryBuilders.termQuery("title", "head")).addSort("id", SortOrder.ASC);

//		高亮
//		searchRequestBuilder.addHighlightedField("title");
//		searchRequestBuilder.setHighlighterPreTags("<em>");
//		searchRequestBuilder.setHighlighterPostTags("</em>");

		SearchResponse response = searchRequestBuilder.get();

		SearchHits hits = response.getHits();
		System.out.println("查询结果有多少条：" + hits.getTotalHits());

		Iterator<SearchHit> iterator = hits.iterator();
		while (iterator.hasNext()) {
			SearchHit next = iterator.next();
			System.out.println(next.getSourceAsString());

			Map<String, HighlightField> highlightFields = next.getHighlightFields();
			HighlightField highlightField = highlightFields.get("title");
			Text[] fragments = highlightField.fragments();
			Optional<String> title = Arrays.stream(fragments).map(fragment -> fragment.string())
					.reduce((acc ,text) -> acc + text );
			System.out.println(title.get());
		}
	}

}
