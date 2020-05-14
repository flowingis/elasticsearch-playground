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
import org.elasticsearch.index.query.QueryBuilders;
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

        switch (queryData.getSearchType()) {
            case MATCH_ALL_QUERY:
                return searchMatchAllQuery(queryData);
        }

        return null;
    }

    private SearchResult searchMatchAllQuery(QueryData queryData) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

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

        searchRequest.indices(serverConfiguration.getSearchIndex());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        return SearchResult.fromSearchResponse(searchResponse);
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
    }

}
