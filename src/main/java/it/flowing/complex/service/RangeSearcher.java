package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.QueryData;
import it.flowing.complex.model.RangeOperator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.Map;

public class RangeSearcher implements Searcher {

    @Override
    public void checkPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData.getTermName());
        Preconditions.checkNotNull(queryData.getRangeValues());
    }

    @Override
    public QueryBuilder getQueryBuilder(QueryData queryData) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(queryData.getTermName());
        setValueForRange(queryData, rangeQueryBuilder);
        return rangeQueryBuilder;
    }

    static void setValueForRange(QueryData queryData, RangeQueryBuilder rangeQueryBuilder) {
        for (Map.Entry<RangeOperator, Object> rangeValue : queryData.getRangeValues().entrySet()) {
            if (rangeValue.getKey().equals(RangeOperator.GT)) {
                rangeQueryBuilder.gt(rangeValue.getValue());
            } else if (rangeValue.getKey().equals(RangeOperator.GTE)) {
                rangeQueryBuilder.gte(rangeValue.getValue());
            } else if (rangeValue.getKey().equals(RangeOperator.LT)) {
                rangeQueryBuilder.lt(rangeValue.getValue());
            } else if (rangeValue.getKey().equals(RangeOperator.LTE)) {
                rangeQueryBuilder.lte(rangeValue.getValue());
            }
        }
    }
}
