package it.flowing.complex.service.elasticsearch;

import com.google.common.base.Preconditions;
import it.flowing.complex.model.*;
import it.flowing.complex.service.configuration.ServerConfiguration;
import it.flowing.complex.service.searcher.*;
import it.flowing.complex.model.CreateDocumentResponse;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

    public CreateDocumentResponse indexDocument(String indexName, byte[] content, Map<String, Object> metadata) throws IOException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkArgument(content.length > 0);

        if (null == metadata) {
            metadata = new HashMap<>();
        }
        metadata.put("data", Base64.getEncoder().encodeToString(content));

        IndexRequest indexRequest = new IndexRequest(indexName).source(metadata);
        indexRequest.setPipeline("attachment");
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        CreateDocumentResponse createDocumentResponse = CreateDocumentResponse.builder()
                .status(indexResponse.status())
                .id(indexResponse.getId())
                .build();

        return createDocumentResponse;
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

    public boolean deleteIndex(String indexName, Optional<Map<String, Object>> configuration)
            throws IOException, ElasticsearchException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);

        // TODO: gestire parametro configuration

        try {
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            return deleteIndexResponse.isAcknowledged();
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    public CreateDocumentResponse createDocument(String indexName, Map<String, Object> metadata, Optional<String> documentId)
            throws IllegalArgumentException, IOException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkNotNull(metadata);

        IndexRequest indexRequest = new IndexRequest(indexName).source(metadata);
        if (documentId.isPresent()) {
            indexRequest.id(documentId.get());
        }

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        CreateDocumentResponse createDocumentResponse = CreateDocumentResponse.builder()
                .status(indexResponse.status())
                .id(indexResponse.getId())
                .build();

        return createDocumentResponse;
    }

    public Document getDocument(String indexName, String documentId, Optional<Map<String, Object>> configuration) throws IOException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkNotNull(documentId);
        Preconditions.checkArgument(!documentId.isEmpty());

        GetRequest getRequest = new GetRequest(indexName, documentId);
        //TODO: Gestire parametro configuration
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        if (getResponse.isExists()) {
            return Document
                    .builder()
                    .fields(getResponse.getFields())
                    .source(getResponse.getSource())
                    .build();
        }

        return null;
    }

    public boolean existsDocument(String indexName, String documentId) throws IOException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkNotNull(documentId);
        Preconditions.checkArgument(!documentId.isEmpty());

        GetRequest getRequest = new GetRequest(indexName, documentId);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    public UpdateDocumentResponse updateDocument(String indexName,
                                                 String documentId,
                                                 Map<String, Object> metadataToUpdate,
                                                 Optional<Map<String, Object>> configuration)
            throws IllegalArgumentException, IOException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkNotNull(documentId);
        Preconditions.checkArgument(!documentId.isEmpty());
        Preconditions.checkNotNull(metadataToUpdate);

        if (!this.existsDocument(indexName, documentId)) {
            return null;
        }

        // TODO: testare funzionamento con le varie opzioni di configuration
        UpdateRequest updateRequest = new UpdateRequest(indexName, documentId)
                .doc(metadataToUpdate);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        UpdateDocumentResponse updateDocumentResponse = UpdateDocumentResponse.builder()
                .status(updateResponse.status())
                .id(updateResponse.getId())
                .build();

        return updateDocumentResponse;
    }

    public DeleteDocumentResponse deleteDocument(String indexName,
                                                 String documentId,
                                                 Optional<Map<String, Object>> configuration)
            throws IllegalArgumentException, IOException {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkNotNull(documentId);
        Preconditions.checkArgument(!documentId.isEmpty());

        if (!this.existsDocument(indexName, documentId)) {
            return null;
        }

        // TODO: testare funzionamento con le varie opzioni di configuration
        DeleteRequest deleteRequest = new DeleteRequest(indexName, documentId);

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        DeleteDocumentResponse deleteDocumentResponse = DeleteDocumentResponse.builder()
                .status(deleteResponse.status())
                .id(deleteResponse.getId())
                .build();

        return deleteDocumentResponse;
    }

}
