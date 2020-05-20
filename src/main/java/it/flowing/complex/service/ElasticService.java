package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.*;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;

@ApplicationScoped
@NoArgsConstructor
public class ElasticService {

    @Inject
    private ServerConfiguration serverConfiguration;

    private RestHighLevelClient client;

    @Inject
    public ElasticService(ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    public void openConnection() {
        Preconditions.checkNotNull(serverConfiguration);

        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(serverConfiguration.getHost(), serverConfiguration.getPort())
                )
        );
    }

    public void closeConnection() throws IOException {
        if (null != client) {
            client.close();
        }
    }

    public SearchResult search(QueryData queryData) throws IOException {
        checkSearchPreconditions(queryData);

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        switch (queryData.getSearchType()) {
            case MATCH_ALL_QUERY:
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
                break;
            case TERM_QUERY:
                searchSourceBuilder.query(QueryBuilders.termQuery(queryData.getTermName(), queryData.getTermValue()));
                break;
            case TERMS_QUERY:
                searchSourceBuilder.query(QueryBuilders.termsQuery(queryData.getTermName(), queryData.getTermValues().toArray()));
                break;
            case EXISTS_QUERY:
                searchSourceBuilder.query(QueryBuilders.existsQuery(queryData.getTermName()));
                break;
            case FUZZY_QUERY:
                searchSourceBuilder.query(QueryBuilders.fuzzyQuery(queryData.getTermName(), queryData.getTermValue()));
                break;
            case RANGE_QUERY:
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(queryData.getTermName());
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
                searchSourceBuilder.query(rangeQueryBuilder);
                break;
            case NESTED_QUERY:
                NestedQueryBuilder nestedQueryBuilder = QueryBuilders
                        .nestedQuery(
                                queryData.getTermName(),
                                QueryBuilders.termQuery(queryData.getSubTermName(),queryData.getSubTermValue()), ScoreMode.None);
                searchSourceBuilder.query(nestedQueryBuilder);
                break;
        }

        addPagination(queryData, searchSourceBuilder);

        if (queryData.getTimeout().isPresent()) {
            searchSourceBuilder.timeout(queryData.getTimeout().get());
        }

        queryData.getSortingCriteria().forEach(searchSourceBuilder::sort);

        if (queryData.getExcludeFields().isPresent() && queryData.getIncludeFields().isPresent()) {
            searchSourceBuilder.fetchSource(queryData.getIncludeFields().get(), queryData.getExcludeFields().get());
        }

        addHighlightFields(queryData, searchSourceBuilder);

        addAggregations(queryData, searchSourceBuilder);

        addSuggestions(queryData, searchSourceBuilder);

        addSearchIndex(queryData, searchRequest);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        return SearchResult.fromSearchResponse(searchResponse);
    }

    private void addSearchIndex(QueryData queryData, SearchRequest searchRequest) {
        if (queryData.getSearchIndex().isPresent()) {
            searchRequest.indices(queryData.getSearchIndex().get());
        } else {
            searchRequest.indices(serverConfiguration.getSearchIndex());
        }
    }

    private void addPagination(QueryData queryData, SearchSourceBuilder searchSourceBuilder) {
        if (queryData.getFrom().isPresent()) {
            searchSourceBuilder.from(queryData.getFrom().get());
        }

        if (queryData.getSize().isPresent()) {
            searchSourceBuilder.size(queryData.getSize().get());
        }
    }

    private void addAggregations(QueryData queryData, SearchSourceBuilder searchSourceBuilder) {
        // TODO: Verificare la possibilità di effettuare aggregazioni multiple
        for(Map<String, Object> aggregation : queryData.getAggregationInfo()) {
            TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(aggregation.get(AggregationInfoFieldName.TERM.toString()).toString())
                    .field(aggregation.get(AggregationInfoFieldName.FIELD.toString()).toString());
            switch ((AggregationType)aggregation.get(AggregationInfoFieldName.AGGREGATION_TYPE.toString())) {
                case AVG:
                    termsAggregationBuilder.subAggregation(AggregationBuilders.avg(aggregation.get(AggregationInfoFieldName.SUB_NAME.toString()).toString())
                            .field(aggregation.get(AggregationInfoFieldName.SUB_FIELD.toString()).toString()));
                    break;
            }
            searchSourceBuilder.aggregation(termsAggregationBuilder);
        }
    }

    private void addSuggestions(QueryData queryData, SearchSourceBuilder searchSourceBuilder) {
        SuggestBuilder suggestBuilder = new SuggestBuilder();

        for(Pair<String, String> suggestion : queryData.getSuggestions()) {
            SuggestionBuilder termSuggestionBuilder = SuggestBuilders
                    .termSuggestion(suggestion.getLeft())
                    .text(suggestion.getRight());

            suggestBuilder.addSuggestion("suggest_" + suggestion.getLeft(), termSuggestionBuilder);
        }

        searchSourceBuilder.suggest(suggestBuilder);
    }

    private void addHighlightFields(QueryData queryData, SearchSourceBuilder searchSourceBuilder) {
        for (Map<String, Object> highlightField : queryData.getHighlightFields()) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field field = new HighlightBuilder.Field(highlightField.get(HighlightFieldType.NAME.toString()).toString());
            if (highlightField.containsKey(HighlightFieldType.TYPE.toString())) {
                field.highlighterType(highlightField.get(HighlightFieldType.TYPE.toString()).toString());
            }
            highlightBuilder.field(field);
            searchSourceBuilder.highlighter(highlightBuilder);
        }
    }

    private void checkSearchPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData);

        switch (queryData.getSearchType()) {
            case TERM_QUERY:
                Preconditions.checkNotNull(queryData.getTermName());
                Preconditions.checkNotNull(queryData.getTermValue());
                break;
            case TERMS_QUERY:
                Preconditions.checkNotNull(queryData.getTermName());
                Preconditions.checkArgument(queryData.getTermValues().size() > 0);
                break;
            case EXISTS_QUERY:
                Preconditions.checkNotNull(queryData.getTermName());
                break;
            case FUZZY_QUERY:
                Preconditions.checkNotNull(queryData.getTermName());
                Preconditions.checkNotNull(queryData.getTermValue());
                break;
            case RANGE_QUERY:
                Preconditions.checkNotNull(queryData.getTermName());
                Preconditions.checkNotNull(queryData.getRangeValues());
                break;
            case NESTED_QUERY:
                Preconditions.checkNotNull(queryData.getTermName());
                Preconditions.checkNotNull(queryData.getSubTermName());
                Preconditions.checkNotNull(queryData.getSubTermValue());
                break;
        }
    }

}
