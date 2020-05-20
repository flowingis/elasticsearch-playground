package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.QueryData;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class NestedSearcher implements Searcher {

    @Override
    public void checkPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData.getTermName());
        Preconditions.checkNotNull(queryData.getSubTermName());
        Preconditions.checkNotNull(queryData.getSubTermValue());
    }

    @Override
    public QueryBuilder getQueryBuilder(QueryData queryData) {
        return QueryBuilders.nestedQuery(
                    queryData.getTermName(),
                    QueryBuilders.termQuery(queryData.getSubTermName(), queryData.getSubTermValue()),
                    ScoreMode.None);
    }
}
