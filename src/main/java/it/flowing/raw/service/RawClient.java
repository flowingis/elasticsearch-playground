package it.flowing.raw.service;

import com.google.common.base.Preconditions;
import it.flowing.raw.model.*;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class RawClient {

    private RestHighLevelClient client;

    private RawClient(RestHighLevelClient client) {
        this.client = client;
    }

    public static RawClient newClient(String host, int port) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)
                )
        );
        return new RawClient(client);
    }

    public void close() throws IOException {
        client.close();
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

    public CreateDocumentResponse createDocument(String indexName, Map<String, Object> metadata, Optional<String> documentId )
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
}