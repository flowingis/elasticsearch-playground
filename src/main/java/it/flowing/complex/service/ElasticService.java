package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.QueryData;
import it.flowing.complex.model.SearchResult;
import lombok.NoArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;

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

        if (queryData.getFrom().isPresent()) {
            searchSourceBuilder.from(queryData.getFrom().get());
        }

        if (queryData.getSize().isPresent()) {
            searchSourceBuilder.size(queryData.getSize().get());
        }

        if (queryData.getTimeout().isPresent()) {
            searchSourceBuilder.timeout(queryData.getTimeout().get());
        }

        for (SortBuilder sortBuilder : queryData.getSortingCriteria()) {
            searchSourceBuilder.sort(sortBuilder);
        }

        if (queryData.getExcludeFields().isPresent() && queryData.getIncludeFields().isPresent()) {
            searchSourceBuilder.fetchSource(queryData.getIncludeFields().get(), queryData.getExcludeFields().get());
        }

        searchRequest.indices(serverConfiguration.getSearchIndex());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return SearchResult.fromSearchResponse(searchResponse);
    }

    private void checkSearchPreconditions(QueryData queryData) {
        Preconditions.checkNotNull(queryData);
    }

}
