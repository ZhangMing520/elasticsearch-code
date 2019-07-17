package util;

import com.carrotsearch.hppc.ObjectLookupContainer;
import domain.Pair;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion.FuzzyOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhangming
 * @date 2019/5/26 22:01
 */
public class ESUtils {

    private static final Logger logger = LoggerFactory.getLogger(ESUtils.class);

    // todo className can change
    private static final String[] PRE_TAGS = {"<span class='className'>"};
    private static final String[] POST_TAGS = {"</span>"};

    private static final String TOP_N_AGG_NAME = "top_n";
    private static final String DEFAULT_TYPE_NAME = "doc";
    private static final String ES_VERSION = "_version";

    private static final String IK_ANALYZER = "ik_smart";


    public static String insert(TransportClient client, String index, Map<String, Object> source) throws Exception {
        return insert(client, index, null, source);
    }

    public static String insert(TransportClient client, String index, String id, Map<String, Object> source) throws Exception {
        IndexResponse response = client.prepareIndex(index, DEFAULT_TYPE_NAME, id)
                .setSource(source, XContentType.SMILE).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        if (response.getResult() != DocWriteResponse.Result.CREATED) {
            throw new Exception("failed to create into " + index);
        }

        return response.getId();
    }

    public static List<BulkItemResponse.Failure> bulkInsertWithId(TransportClient client, final String index,
                                                                  List<Tuple<String, Map<String, Object>>> list) {
        final BulkRequestBuilder builder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        list.forEach(t -> builder.add(new IndexRequest(index, DEFAULT_TYPE_NAME, t.v1()).source(t.v2(), XContentType.SMILE)));

        List<BulkItemResponse.Failure> failureList = new ArrayList<>();
        if (builder.numberOfActions() > 0) {
            BulkResponse response = builder.execute().actionGet(10, TimeUnit.SECONDS);
            BulkItemResponse[] items = response.getItems();
            for (BulkItemResponse itemResponse : items) {
                if (itemResponse.isFailed()) {
                    // todo log
                    failureList.add(itemResponse.getFailure());
                }
            }
        }

        return failureList;
    }

    public static void bulkInsert(TransportClient client, String index, List<Map<String, Object>> list) {
        BulkRequestBuilder builder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map<String, Object> map : list) {
            builder.add(new IndexRequest(index, DEFAULT_TYPE_NAME).source(map, XContentType.SMILE));
        }

        if (builder.numberOfActions() > 0) {
            BulkResponse response = builder.execute().actionGet();
            BulkItemResponse[] items = response.getItems();
            for (BulkItemResponse itemResponse : items) {
                if (itemResponse.isFailed()) {
                    // todo log

                }
            }
        }
    }

    public static BulkItemResponse[] bulkInsertOrUpdate(TransportClient client, String index,
                                                        List<Tuple<String, Map<String, Object>>> list) {
        BulkRequestBuilder builder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Tuple<String, Map<String, Object>> tuple : list) {
            String id = tuple.v1();
            Map<String, Object> sourceMap = tuple.v2();
            IndexRequest indexRequest = new IndexRequest(index, DEFAULT_TYPE_NAME);
            if (StringUtils.isNotBlank(id)) {
                indexRequest.id(id);
            }

            if (sourceMap.containsKey(ES_VERSION)) {
                indexRequest.version(Long.valueOf(sourceMap.remove(ES_VERSION).toString()));
            }

            indexRequest.source(sourceMap, XContentType.SMILE);

            builder.add(indexRequest);
        }

        if (builder.numberOfActions() > 0) {
            BulkResponse response = builder.execute().actionGet();
            BulkItemResponse[] items = response.getItems();
            for (BulkItemResponse itemResponse : items) {
                if (itemResponse.isFailed()) {
                    // todo log
                }
            }
            return items;
        }
        return null;
    }

    public static boolean updateById(TransportClient client, String index, String id, Map<String, Object> source) {
        UpdateResponse response = client.prepareUpdate(index, DEFAULT_TYPE_NAME, id).setDoc(source, XContentType.SMILE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).setFetchSource(false)
                .setDetectNoop(false).get();

        return response.getResult() == DocWriteResponse.Result.UPDATED;
    }

    public static boolean updateById(TransportClient client, String index, String id, long version, Map<String, Object> source) {
        UpdateResponse response = client.prepareUpdate(index, DEFAULT_TYPE_NAME, id).setVersion(version)
                .setDoc(source, XContentType.SMILE).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setFetchSource(false).setDetectNoop(false).get();

        return response.getResult() == DocWriteResponse.Result.UPDATED;
    }

    public static List<String> updateByIds(TransportClient client, List<String> indexList,
                                           Map<String, Object> docValues, List<List<String>> indexIdList) {
        BulkRequestBuilder builder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int size = indexList.size();
        for (int i = 0; i < size; i++) {
            String index = indexList.get(i);
            List<String> idList = indexIdList.get(i);

            for (String id : idList) {
                UpdateRequest updateRequest = new UpdateRequest(index, DEFAULT_TYPE_NAME, id).doc(docValues);
                builder.add(updateRequest);
            }
        }
        List<String> messageList = new ArrayList<>();
        if (builder.numberOfActions() > 0) {
            BulkResponse response = builder.execute().actionGet();
            BulkItemResponse[] items = response.getItems();
            for (BulkItemResponse itemResponse : items) {
                if (itemResponse.isFailed()) {
                    // todo log
                    messageList.add(itemResponse.getFailureMessage());
                }
            }
        }

        return messageList;
    }

    public static long count(TransportClient client, String... index) {
        SearchResponse response = client.prepareSearch(index).setFetchSource(false)
                .setSize(0).setTypes(DEFAULT_TYPE_NAME).execute().actionGet();

        return response.getHits().getTotalHits();
    }

    public static List<Map<String, Object>> getById(TransportClient client, String index, String id) {
        GetResponse response = client.prepareGet(index, DEFAULT_TYPE_NAME, id).setFetchSource(true)
                .execute().actionGet();
        if (!response.isExists()) {
            return Collections.emptyList();
        }

        return Collections.singletonList(response.getSourceAsMap());
    }

    public static List<Map<String, Object>> getByIds(TransportClient client, List<String> indexList, List<List<String>> indexIdList) {
        MultiGetRequestBuilder builder = client.prepareMultiGet();
        int size = indexList.size();
        for (int i = 0; i < size; i++) {
            List<String> idList = indexIdList.get(i);
            if (CollectionUtils.isEmpty(idList)) {
                continue;
            }

            builder.add(indexList.get(i), DEFAULT_TYPE_NAME, idList);
        }

        if (builder.request().getItems().isEmpty()) {
            return Collections.emptyList();
        }
        MultiGetResponse response = builder.execute().actionGet();
        MultiGetItemResponse[] responses = response.getResponses();
        if (responses == null || responses.length == 0) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> list = Stream.of(responses).filter(r -> !r.isFailed())
                .map(item -> item.getResponse().getSourceAsMap()).collect(Collectors.toList());

        return list;
    }

    public static List<Map<String, Object>> getByFields(TransportClient client, String index, Map<String, Object> fields)
            throws Exception {
        if (fields.size() != 0) {
            return getByFields(client, index, Collections.singletonList(fields), null, null, 0, 0, null);
        }
        return Collections.emptyList();
    }

    public static List<Map<String, Object>> getByFields(TransportClient client, String index, List<Map<String, Object>> fieldsList,
                                                        List<Tuple<String, SortOrder>> sortFields,
                                                        String[] includes, int page, int limit, String keyword) throws Exception {
        if (fieldsList == null || fieldsList.size() == 0) {
            return Collections.emptyList();
        }

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        HighlightBuilder hb = null;
        if (StringUtils.isNoneBlank(keyword)) {
            BoolQueryBuilder keywordQuery = QueryBuilders.boolQuery();
            queryBuilder.must(keywordQuery);

            keywordQuery.should(QueryBuilders.multiMatchQuery(keyword, "*")
                    .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                    .operator(Operator.AND).tieBreaker(0.3f).analyzer(IK_ANALYZER));

            hb = new HighlightBuilder().field("*").preTags(PRE_TAGS).postTags(POST_TAGS).requireFieldMatch(true);
        }

        for (Map<String, Object> fields : fieldsList) {
            BoolQueryBuilder subQueryBuilder = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                String k = entry.getKey();
                Object v = entry.getValue();
                if (v instanceof List) {
                    if (isNestedField(k)) {
                        subQueryBuilder.must(QueryBuilders.nestedQuery(getParentPath(k),
                                QueryBuilders.termsQuery(k, (List) v), ScoreMode.None).ignoreUnmapped(true));
                    } else {
                        subQueryBuilder.must(QueryBuilders.termsQuery(k, (List) v));
                    }
                } else {
                    if (isNestedField(k)) {
                        // if this is fuzzy query , QueryBuilders.wildcardQuery replace QueryBuilders.termQuery
                        // if this is prefix query , QueryBuilders.prefixQuery replace QueryBuilders.termQuery
                        // if this is date , QueryBuilders.rangeQuery replace QueryBuilders.termQuery

                        // but wildcardQuery is slow ,not suggest start with * , like *keyword or *keyword*
                        subQueryBuilder.must(QueryBuilders.nestedQuery(getParentPath(k), QueryBuilders.termQuery(k, v),
                                ScoreMode.None).ignoreUnmapped(true));
                    } else {
                        subQueryBuilder.must(QueryBuilders.termQuery(k, v));
                    }
                }
            }
            queryBuilder.should(subQueryBuilder);
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(DEFAULT_TYPE_NAME)
                .setSearchType(SearchType.DEFAULT).setQuery(queryBuilder).highlighter(hb).setFetchSource(true)
                .setFetchSource(includes, null).setVersion(true).setExplain(false);

        if (CollectionUtils.isNotEmpty(sortFields)) {
            for (Tuple<String, SortOrder> t : sortFields) {
                String field = t.v1();
                SortOrder sortOrder = t.v2();
                if (isNestedField(field)) {
                    searchRequestBuilder.addSort(SortBuilders.fieldSort(join(field, KEY)).order(sortOrder)
                            .setNestedSort(new NestedSortBuilder(getParentPath(field))));
                } else {
                    searchRequestBuilder.addSort(field, sortOrder);
                }
            }
        }

        searchRequestBuilder.addSort(new ScoreSortBuilder());
        if (page > 0 && limit > 0) {
            searchRequestBuilder.setFrom((page - 1) * limit).setSize(limit);
        }

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        if (searchResponse.status() != RestStatus.OK) {
            StringBuffer sb = new StringBuffer();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                sb.append(failure.reason()).append("\n");
            }

            throw new Exception(sb.toString());
        }

        int totalSize = (int) searchResponse.getHits().getTotalHits();
        if (totalSize == 0) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> list = new ArrayList<>(totalSize);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit searchHit : hits) {
            Map<String, Object> source = searchHit.getSourceAsMap();

            if (hb != null) {
                Map<String, String> highlights = searchHit.getHighlightFields().entrySet().stream().collect(
                        Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().fragments()[0].toString()));

                // todo modify by zhangmingp
                source.put("highlights", highlights);
            }
            list.add(source);
        }

        return list;
    }

    public static SearchResponse getByFieldsExists(TransportClient client, String index, String path, String field) {
        QueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(field);
        if (isNestedField(field)) {
            existsQueryBuilder = QueryBuilders.nestedQuery(path, existsQueryBuilder, ScoreMode.None);
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(DEFAULT_TYPE_NAME)
                .setSearchType(SearchType.DEFAULT).setQuery(existsQueryBuilder).setFetchSource(true)
                .setVersion(true).setExplain(false);

        return searchRequestBuilder.execute().actionGet();
    }


    public static boolean exist(TransportClient client, String[] indices, String[] fields, Object value) {
        MultiSearchRequestBuilder searchRequestBuilder = client.prepareMultiSearch();
        int length = indices.length;
        for (int i = 0; i < length; i++) {
            String field = fields[i];
            QueryBuilder filter = QueryBuilders.termQuery(field, value);
            if (isNestedField(field)) {
                filter = QueryBuilders.nestedQuery(getParentPath(field), filter, ScoreMode.None);
            }

            searchRequestBuilder.add(client.prepareSearch(indices[i]).setTypes(DEFAULT_TYPE_NAME)
                    .setSearchType(SearchType.DEFAULT).setPostFilter(filter).setFetchSource(false).setExplain(false));
        }

        MultiSearchResponse response = searchRequestBuilder.get();
        return Stream.of(response.getResponses())
                .anyMatch(item -> !item.isFailure() && item.getResponse().getHits().getTotalHits() > 0);
    }

    public static boolean exist(TransportClient client, String index, String field, Object value) throws Exception {
        QueryBuilder filter = QueryBuilders.termQuery(field, value);
        if (isNestedField(field)) {
            filter = QueryBuilders.nestedQuery(getParentPath(field), filter, ScoreMode.None);
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(DEFAULT_TYPE_NAME)
                .setSearchType(SearchType.DEFAULT)
                .setPostFilter(filter).setFetchSource(false).setExplain(false);

        SearchResponse response = searchRequestBuilder.get();
        if (response.status() != RestStatus.OK) {
            StringBuffer sb = new StringBuffer();
            for (ShardSearchFailure failure : response.getShardFailures()) {
                sb.append(failure.reason()).append("\n");
            }

            throw new Exception(sb.toString());
        }

        return response.getHits().getTotalHits() > 0;
    }

    public static boolean deleteById(TransportClient client, String index, String id) {
        DeleteResponse response = client.prepareDelete(index, DEFAULT_TYPE_NAME, id)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        return response.getResult() == DocWriteResponse.Result.DELETED;
    }

    public static void bulkDelete(TransportClient client, List<String> indexList, List<String> idList) {
        BulkRequestBuilder builder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String index : indexList) {
            for (String id : idList) {
                builder.add(new DeleteRequest(index, DEFAULT_TYPE_NAME, id));
            }
        }
        if (builder.numberOfActions() > 0) {
            // todo log
            BulkResponse response = builder.get();
            BulkItemResponse[] responses = response.getItems();
            for (BulkItemResponse itemResponse : responses) {
                if (itemResponse.isFailed()) {
                    // todo log
                }
            }
        }
    }

    public static List<String> suggest(TransportClient client, String index, String[] fields, String input, int size) {
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        for (String field : fields) {
            suggestBuilder.addSuggestion(field, new CompletionSuggestionBuilder(field)
                    .size(size).prefix(input, FuzzyOptions.builder().setUnicodeAware(true).build())
                    .skipDuplicates(true));
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setFetchSource(false)
                .suggest(suggestBuilder).setExplain(false);

        SearchResponse response = searchRequestBuilder.get();
        if (response.getSuccessfulShards() <= 0 || response.getSuggest() == null) {
            return Collections.emptyList();
        }

        Suggest suggest = response.getSuggest();
        if (suggest.size() == 1) {
            CompletionSuggestion suggestion = (CompletionSuggestion) suggest.iterator().next();
            return suggestion.getOptions().stream().map(option -> option.getText().toString()).collect(Collectors.toList());
        }

        List<List<CompletionSuggestion.Entry.Option>> list = new ArrayList<>(suggest.size());
        suggest.forEach(suggestion -> {
            CompletionSuggestion completionSuggestion = (CompletionSuggestion) suggestion;
            List<CompletionSuggestion.Entry.Option> options = completionSuggestion.getOptions();
            if (options.size() > 0) {
                list.add(options);
            }
        });

        if (list.size() == 0) {
            return Collections.emptyList();
        }

        List<String> result = list.stream().flatMap(subList -> subList.stream())
                .sorted((o1, o2) -> Float.compare(o1.getScore(), o2.getScore()))
                .map(o -> o.getText().toString()).distinct().collect(Collectors.toList());

        return result;
    }

    public static List<Pair<String, Long>> topN(TransportClient client, String index, String field, int size)
            throws Exception {
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms(TOP_N_AGG_NAME).field(field)
                .size(size);
        boolean nestedField = isNestedField(field);

        if (nestedField) {
            aggregationBuilder = AggregationBuilders.nested(TOP_N_AGG_NAME, getParentPath(field))
                    .subAggregation(aggregationBuilder);
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).addAggregation(aggregationBuilder)
                .setTypes(DEFAULT_TYPE_NAME).setSearchType(SearchType.DEFAULT).setSize(0)
                .setFetchSource(false).setExplain(false);

        SearchResponse response = searchRequestBuilder.get();
        if (response.status() != RestStatus.OK) {
            StringBuffer sb = new StringBuffer();
            for (ShardSearchFailure failure : response.getShardFailures()) {
                sb.append(failure.reason()).append("\n");
            }
            throw new Exception(sb.toString());
        }

        Aggregations aggregations = response.getAggregations();
        if (nestedField) {
            aggregations = ((Nested) aggregations.get(TOP_N_AGG_NAME)).getAggregations();
        }

        Terms terms = aggregations.get(TOP_N_AGG_NAME);
        if (terms != null && CollectionUtils.isNotEmpty(terms.getBuckets())) {
            return terms.getBuckets().stream().map(bucket -> new Pair<>(bucket.getKeyAsString(), bucket.getDocCount()))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public static double max(TransportClient client, String index, String field) throws Exception {
        AggregationBuilder aggregationBuilder = AggregationBuilders.max(field).field(field);
        boolean nestedField = isNestedField(field);

        if (nestedField) {
            aggregationBuilder = AggregationBuilders.nested(field, getParentPath(field))
                    .subAggregation(aggregationBuilder);
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).addAggregation(aggregationBuilder)
                .setTypes(DEFAULT_TYPE_NAME).setSearchType(SearchType.DEFAULT).setSize(0)
                .setFetchSource(false).setExplain(false);

        SearchResponse response = searchRequestBuilder.get();
        if (response.status() != RestStatus.OK) {
            StringBuffer sb = new StringBuffer();
            for (ShardSearchFailure failure : response.getShardFailures()) {
                sb.append(failure.reason()).append("\n");
            }
            throw new Exception(sb.toString());
        }

        Aggregations aggregations = response.getAggregations();
        if (nestedField) {
            aggregations = ((Nested) aggregations.get(field)).getAggregations();
        }

        Max max = aggregations.get(field);
        return max.getValue();
    }


    public static void clearIndexData(TransportClient client, String index) throws Exception {
        DeleteByQueryRequestBuilder builder = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);

        builder.filter(new MatchAllQueryBuilder());
        try {
            builder.source(index).execute().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new Exception("清除 " + index + " 数据失败。", e);
        }

    }

    public static void createAlias(TransportClient client, String alias, String index) throws Exception {
        IndicesAdminClient adminClient = client.admin().indices();
        if (adminClient.prepareAliasesExist(alias).execute().actionGet().exists()) {
            ObjectLookupContainer<String> aliasKeys = adminClient.prepareGetAliases().setIndices(alias)
                    .execute().actionGet().getAliases().keys();

            if (aliasKeys.size() != 0 && !aliasKeys.contains(index)) {
                throw new Exception(index + " 创建别名失败，其它索引 " + aliasKeys.toString() + " 已经拥有别名 【" + alias + "】");
            }

            return;
        }

        adminClient.prepareAliases().addAlias(index, alias).execute().actionGet();
    }

    public static boolean changeAliasTo(TransportClient client, String alias, String oldIndex, String newIndex) {
        IndicesAdminClient adminClient = client.admin().indices();
        if (adminClient.prepareAliasesExist(alias).execute().actionGet().exists()) {

            if (adminClient.prepareAliases().removeAlias(oldIndex, alias).execute().actionGet().isAcknowledged()) {
                return adminClient.prepareAliases().addAlias(newIndex, alias).execute().actionGet()
                        .isAcknowledged();
            }
        }
        return false;
    }

    public static Map getIndexPropertiesOfMappings(TransportClient client, String indexName) {
        Map propertiesOfMappings = (Map) client.admin().cluster().prepareState().execute().actionGet().getState()
                .getMetaData().getIndices().get(indexName).getMappings().get(DEFAULT_TYPE_NAME)
                .getSourceAsMap().get("properties");

        return propertiesOfMappings;
    }

    public static String getRealIndexByAlias(TransportClient client, String alias) throws Exception {
        String realIndexName = client.admin().indices().prepareGetAliases().setIndices(alias).execute().get()
                .getAliases().keysIt().next();
        return realIndexName;
    }

    public static List<Map<String, Object>> scrollSearch(TransportClient client, String index) throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(DEFAULT_TYPE_NAME)
                .setSize(500).setScroll(TimeValue.timeValueMinutes(1)).setSearchType(SearchType.DEFAULT).setExplain(false);

        SearchResponse response = searchRequestBuilder.get();
        if (response.status() != RestStatus.OK) {
            StringBuffer sb = new StringBuffer();
            for (ShardSearchFailure failure : response.getShardFailures()) {
                sb.append(failure.reason()).append("\n");
            }
            throw new Exception(sb.toString());
        }

        int totalSize = (int) response.getHits().getTotalHits();
        List<Map<String, Object>> result = new ArrayList<>(totalSize);
        scrollOutput(response, result);

        for (int i = 0; i < totalSize; i++) {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();
            i += response.getHits().getHits().length;
            scrollOutput(response, result);
        }

        // todo  in finally
        if (response != null && StringUtils.isNotBlank(response.getScrollId())) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(response.getScrollId());
            client.clearScroll(clearScrollRequest);
        }

        return result;
    }

    private static void scrollOutput(SearchResponse response, List<Map<String, Object>> list) {
        SearchHits searchHits = response.getHits();
        for (SearchHit hit : searchHits) {
            list.add(hit.getSourceAsMap());
        }
    }


    ///////////////////////////////////////////////////////////////////////////

    private static final char SEPARATOR = '.';
    private static final String KEY = "key";

    private static boolean isNestedField(String path) {
        int index = path.lastIndexOf(".key");
        if (index < 0) {
            index = path.lastIndexOf(".suggest");
        }
        int i = path.lastIndexOf(SEPARATOR);

        return i > -1 && (index <= -1 || i < index);
    }

    private static String getParentPath(String path) {
        int index = path.lastIndexOf(".key");
        if (index < 0) {
            index = path.lastIndexOf(".suggest");
        }

        if (index > -1) {
            path = path.substring(0, index);
        }

        return path.substring(0, path.lastIndexOf(SEPARATOR));
    }

    private static String join(String... names) {
        return StringUtils.join(names, SEPARATOR);
    }
}
