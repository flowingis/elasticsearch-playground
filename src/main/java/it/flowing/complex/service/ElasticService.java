package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.*;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
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
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
@NoArgsConstructor
public class ElasticService {

    public static final String SUGGEST_PREFIX = "suggest_";
    @Inject
    private ServerConfiguration serverConfiguration;

    private RestHighLevelClient client;

    private static Map<SearchType, Searcher> searcherMap = new HashMap<SearchType, Searcher>() {{
        put(SearchType.MATCH_ALL_QUERY, new MatchAllSearcher());
        put(SearchType.TERM_QUERY, new TermSearcher());
        put(SearchType.TERMS_QUERY, new TermsSearcher());
        put(SearchType.EXISTS_QUERY, new ExistsSearcher());
        put(SearchType.FUZZ_QUERY, new FuzzSearcher());
        put(SearchType.RANGE_QUERY, new RangeSearcher());
        put(SearchType.NESTED_QUERY, new NestedSearcher());
    }};

    private Searcher searcher;

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
        searcher = getSearcher(queryData);

        checkSearchPreconditions(queryData);

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(searcher.getQueryBuilder(queryData));

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

            suggestBuilder.addSuggestion(SUGGEST_PREFIX + suggestion.getLeft(), termSuggestionBuilder);
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

        searcher.checkPreconditions(queryData);
    }

    private Searcher getSearcher(QueryData queryData) {
        Preconditions.checkArgument(searcherMap.containsKey(queryData.getSearchType()));

        return searcherMap.get(queryData.getSearchType());
    }

}
