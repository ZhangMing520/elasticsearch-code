package es.common;

import java.io.IOException;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 嵌套类型
 * 
 * @author zhangming
 * 
 * 
 *
 */
public class ElasticSearchNestedObjectTest {

	private TransportClient client;

	private ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setUp() throws UnknownHostException {
//		连接对象
		client = null ;
	}

	final String indexes = "blog3";
	final String type = "article";

	@Test
	public void testNestedCreateIndex() throws IOException {
		client.admin().indices().prepareCreate(indexes);

		XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties").startObject("id")
				.field("store", "yes").field("type", "integer").endObject().startObject("name").field("store", "yes")
				.field("type", "string").endObject().startObject("commentators").field("store", "yes")
				.field("type", "string").endObject().endObject().endObject().endObject();
	}

}
