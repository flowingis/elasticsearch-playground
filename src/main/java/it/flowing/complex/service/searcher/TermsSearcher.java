package it.flowing.complex.service.searcher;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.QueryData;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class TermsSearcher implements Searcher {

    @Override
    public void checkPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData.getTermName());
        Preconditions.checkArgument(queryData.getTermValues().size() > 0);
    }

    @Override
    public QueryBuilder getQueryBuilder(QueryData queryData) {
        return QueryBuilders.termsQuery(queryData.getTermName(), queryData.getTermValues().toArray());
    }
}
