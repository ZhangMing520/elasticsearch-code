package es.search;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
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
 * <p>
 * An aggregation could be a metrics aggregation or a bucket aggregation.
 */
public class ElasticSearchAggregationTest {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchAggregationTest.class);

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
    public void testAggregation() throws UnknownHostException {
        SearchResponse response = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery())
//                Terms aggregation
                .addAggregation(AggregationBuilders.terms("agg1").field("field"))
//                Date Histogram aggregation (bucket)
                .addAggregation(AggregationBuilders.dateHistogram("agg2")
                                .field("birth").dateHistogramInterval(DateHistogramInterval.YEAR)
//                     Average aggregation (metric)
                                .subAggregation(AggregationBuilders.avg("avg_children")
                                        .field("children"))
                )
                .get();

        // 获取 facet 结果
        Aggregation agg1 = response.getAggregations().get("agg1");
        Aggregation agg2 = response.getAggregations().get("agg2");

        logger.info("search response:[{}]", response);
    }

    @Test
    public void testMaxAndMinAggregation() {
        SearchRequestBuilder requestBuilder = client.prepareSearch();

//        min
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("agg")
                .field("height");

//        max
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("agg")
                .field("height");

//        avg
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("agg")
                .field("height");

//        stats
//       可以获取上述维度内容
        StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders.stats("agg")
                .field("height");

        SearchResponse response = requestBuilder.addAggregation(statsAggregationBuilder).get();
        Stats stats = response.getAggregations().get("agg");
        double avg = stats.getAvg();
        double min = stats.getMin();
        double max = stats.getMax();
        double sum = stats.getSum();
        long count = stats.getCount();

//        extend stats
        ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders.extendedStats("agg")
                .field("height");

        SearchResponse response2 = requestBuilder.addAggregation(extendedStatsAggregationBuilder).get();
        ExtendedStats extendedStats = response2.getAggregations().get("agg");
        double avg2 = extendedStats.getAvg();
        double min2 = extendedStats.getMin();
        double max2 = extendedStats.getMax();
        double sum2 = extendedStats.getSum();
        long count2 = extendedStats.getCount();
        // 平方和
        double sumOfSquares = extendedStats.getSumOfSquares();
        // 方差
        double stdDeviation = extendedStats.getStdDeviation();


        // count
        ValueCountAggregationBuilder countAggregationBuilder = AggregationBuilders.count("agg")
                .field("height");


//        percentile
//        值和百分比的乘积
        PercentilesAggregationBuilder percentilesAggregationBuilder = AggregationBuilders.percentiles("agg")
                .field("height")
//                自己设置百分比
                .percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);

        SearchResponse response1 = requestBuilder.addAggregation(percentilesAggregationBuilder).get();
        Percentiles percentile = response1.getAggregations().get("agg");
        for(Percentile entry : percentile){
            //
            double percent = entry.getPercent();
            double value = entry.getValue();
            logger.info("percent [{}], value [{}]", percent, value);
        }

    }

}
