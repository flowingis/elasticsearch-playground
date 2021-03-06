package it.flowing.complex.service.searcher;

import it.flowing.complex.model.QueryData;
import org.elasticsearch.index.query.QueryBuilder;

public interface Searcher {
    void checkPreconditions(QueryData queryData);
    QueryBuilder getQueryBuilder(QueryData queryData);
}
