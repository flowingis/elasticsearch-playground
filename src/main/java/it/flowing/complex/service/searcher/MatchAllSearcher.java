package it.flowing.complex.service.searcher;

import it.flowing.complex.model.QueryData;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class MatchAllSearcher implements Searcher {

    @Override
    public void checkPreconditions(QueryData queryData) {
    }

    @Override
    public QueryBuilder getQueryBuilder(QueryData queryData) {
        return QueryBuilders.matchAllQuery();
    }

}
