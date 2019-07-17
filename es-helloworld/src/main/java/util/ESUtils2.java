package util;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhangming
 * @date 2019/5/26 22:01
 */
public class ESUtils2 {

    public static String[] existIndices(TransportClient client, String... index) {
        SortedMap<String, AliasOrIndex> lookup = client.admin().cluster().prepareState().execute()
                .actionGet().getState().getMetaData().getAliasAndIndexLookup();


        return Stream.of(index).filter(name -> lookup.containsKey(name)).toArray(String[]::new);
    }

    public static List<BulkItemResponse.Failure> updateFieldById(TransportClient client, String index, String id,
                                                                 Object value, Map<String, Object> updateFields) {
        Map<String, Object> params = new HashMap<>();
        params.putAll(updateFields);

        QueryBuilder queryBuilder = QueryBuilders.termQuery(id, value);
        StringBuilder builder = new StringBuilder();

        params.keySet().forEach(k -> builder.append("ctx._source.").append(k).append("=params.")
                .append(k.replace(".", "_")).append(";"));

        String script = builder.toString();
        params = params.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().replace(".", "_"), e -> e.getValue()));

        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, script, params))
                .source(index).filter(queryBuilder).get();

        return response.getBulkFailures();
    }

    public static void updateFieldArrayElementByCondition(TransportClient client, String field, String conditionField, Object value,
                                                          Map<String, Object> updateFields, String... index) throws Exception {
        Map<String, Object> params = updateFields;
        QueryBuilder queryBuilder = QueryBuilders.termsQuery(conditionField, value);
        if (isNestedField(field)) {
            queryBuilder = QueryBuilders.nestedQuery(getParentPath(field), queryBuilder, ScoreMode.None)
                    .ignoreUnmapped(true);
        }

        StringBuilder builder = new StringBuilder();
        builder.append("List labels = ctx._source.labels;")
                .append("for(Map labelMap:labels){")
                .append("if(labelMap.get('id')==params.get('id')){")
                .append("labelMap.put('name' , params.get('name'))")
                .append("labelMap.put('color', params.get('color'))")
                .append("}")
                .append("}");

        String script = builder.toString();
        updateByCondition(client, queryBuilder, script, params, 1, index);
    }

    public static void bulkDeleteFieldArrayElement(TransportClient client, String field, Map<String, Object> updateFields,
                                                   String... index) throws Exception {
        Map<String, Object> params = updateFields;
        List<Integer> ids = (List<Integer>) params.get("ids");
        if (ids == null) {
            return;
        }

        QueryBuilder queryBuilder = QueryBuilders.termsQuery(join(field, "id"), ids);
        if (isNestedField(field)) {
            queryBuilder = QueryBuilders.nestedQuery(getParentPath(field), queryBuilder, ScoreMode.None)
                    .ignoreUnmapped(true);
        }

        StringBuilder builder = new StringBuilder();
        builder.append("List idList = params.get('ids');")
                .append("List fieldList = ctx._source.").append(field).append(";")
                .append(" int size=0 ; if(fieldList!=null){ size = fieldList.size() }")
                .append("for(def id:idList){")
                .append("for(int i=0 ; i<size ; i++){")
                .append("if(fieldList.get(i).get('id')==id){")
                .append("ctx._source.").append(field).append(".remove(i);")
                .append("break;")
                .append("}")
                .append("}")
                .append("}");

        String script = builder.toString();
        updateByCondition(client, queryBuilder, script, params, 1, index);
    }

    public static List<BulkItemResponse.Failure> updateFields(TransportClient client, String index,
                                                              String field, Object defaultValue) {

        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "ctx._source." + field + "=" + defaultValue,
                        Collections.emptyMap())).source(index).filter(queryBuilder).get();

        return response.getBulkFailures();
    }


    public static void updateByCondition(TransportClient client, QueryBuilder queryBuilder, String script,
                                         Map<String, Object> params, int retryCount, String... index) throws Exception {
        if (index == null || index.length == 0) {
            return;
        }

        if (queryBuilder == null) {
            queryBuilder = QueryBuilders.matchAllQuery();
        }

        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client)
                .script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, script, params))
                .abortOnVersionConflict(false)
                .setMaxRetries(100)
                .source(index).filter(queryBuilder).get();

        List<ScrollableHitSource.SearchFailure> searchFailures = response.getSearchFailures();
        if (CollectionUtils.isNotEmpty(searchFailures)) {
            String failureMessage = searchFailures.stream().limit(3).map(f -> f.getReason().getMessage())
                    .collect(Collectors.joining(";"));
            throw new Exception(failureMessage);
        }

        List<BulkItemResponse.Failure> bulkFailures = response.getBulkFailures();
        if (CollectionUtils.isNotEmpty(bulkFailures)) {
            String failureMessage = bulkFailures.stream().limit(3).map(f -> f.getMessage())
                    .collect(Collectors.joining(";"));
            throw new Exception(failureMessage);
        }

        if (response.getVersionConflicts() > 0) {
            if (retryCount > 0 && retryCount < 15) {
                TimeUnit.SECONDS.sleep(new Random().nextInt(15));

                updateByCondition(client, queryBuilder, script, params, retryCount + 1, index);
            } else if (retryCount >= 15) {
                throw new Exception("failed after retry " + retryCount + " times");
            } else if (retryCount <= 0) {
                throw new Exception("failed to update, no retry");
            }
        }

    }


    private static final String PROPERTIES_FIELD = "properties";
    private static final String BOOST_FIELD = "boost";

    public static boolean updateIndexFieldBoost(TransportClient client, String index, Map<String, Object> fieldsBoost) {
        IndicesAdminClient adminClient = client.admin().indices();
        GetMappingsResponse response = adminClient.prepareGetMappings(index).execute().actionGet();
        Map<String, Object> properties = (Map<String, Object>) response.getMappings().valuesIt().next().valuesIt().next()
                .getSourceAsMap().get(PROPERTIES_FIELD);

        if (properties == null) {
            // todo log
            return false;
        }

        Map<String, Map<String, Object>> propertiesMap = Collections.singletonMap(PROPERTIES_FIELD, new HashMap<>());
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            String key = e.getKey();
            if (!fieldsBoost.containsKey(key)) {
                continue;
            }

            Map<String, Object> fieldProperties = (Map<String, Object>) e.getValue();
            fieldProperties.put(BOOST_FIELD, fieldsBoost.get(key));
            propertiesMap.get(PROPERTIES_FIELD).put(key, fieldProperties);
        }


        if (!adminClient.preparePutMapping(index).setType(DEFAULT_TYPE_NAME).setSource(propertiesMap)
                .execute().actionGet().isAcknowledged()) {
            // todo log
            return false;
        }

        return true;
    }

    public static Map<String, Object> getBoost(TransportClient client, String index) {
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(index).execute().actionGet();
        Map<String, Object> properties = (Map<String, Object>) response.getMappings().valuesIt().next().valuesIt().next()
                .getSourceAsMap().get(PROPERTIES_FIELD);

        if (properties == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> boostSetting = new HashMap<>();
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            Object value = e.getValue();
            if (!(value instanceof Map)) {
                continue;
            }

            Map<String, Object> fieldProperties = (Map<String, Object>) value;
            boostSetting.put(e.getKey(), fieldProperties.getOrDefault(BOOST_FIELD, 1));
        }
        return boostSetting;
    }


    /////////////////////////////////////////////////////////////////
    private static final String DEFAULT_TYPE_NAME = "doc";
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
