package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.QueryData;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class ExistsSearcher implements Searcher {

    @Override
    public void checkPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData.getTermName());
    }

    @Override
    public QueryBuilder getQueryBuilder(QueryData queryData) {
        return QueryBuilders.existsQuery(queryData.getTermName());
    }
}
