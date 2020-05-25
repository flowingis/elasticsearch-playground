package it.flowing.complex.service.searcher;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.BoolQueryRule;
import it.flowing.complex.model.QueryData;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;

public class BoolSearcher implements Searcher {

    @Override
    public void checkPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData.getBoolQueryRules());
    }

    @Override
    public QueryBuilder getQueryBuilder(QueryData queryData) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // TODO: Prevedere vari tipi di query (attualmente solo termQuery)

        if (queryData.getBoolQueryRules().containsKey(BoolQueryRule.MUST.toString())) {
            Map<String, Object> mustValues = queryData.getBoolQueryRules().get(BoolQueryRule.MUST.toString());
            for (Map.Entry<String, Object> mustValueEntry : mustValues.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termQuery(mustValueEntry.getKey(), mustValueEntry.getValue().toString()));
            }
        }

        if (queryData.getBoolQueryRules().containsKey(BoolQueryRule.MUST_NOT.toString())) {
            Map<String, Object> mustValues = queryData.getBoolQueryRules().get(BoolQueryRule.MUST_NOT.toString());
            for (Map.Entry<String, Object> mustValueEntry : mustValues.entrySet()) {
                boolQueryBuilder.mustNot(QueryBuilders.termQuery(mustValueEntry.getKey(), mustValueEntry.getValue().toString()));
            }
        }

        if (queryData.getBoolQueryRules().containsKey(BoolQueryRule.FILTER.toString())) {
            Map<String, Object> mustValues = queryData.getBoolQueryRules().get(BoolQueryRule.FILTER.toString());
            for (Map.Entry<String, Object> mustValueEntry : mustValues.entrySet()) {
                boolQueryBuilder.filter(QueryBuilders.termQuery(mustValueEntry.getKey(), mustValueEntry.getValue().toString()));
            }
        }

        return boolQueryBuilder;
    }
}
