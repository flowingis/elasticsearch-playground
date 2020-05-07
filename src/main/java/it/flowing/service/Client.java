package it.flowing.service;

import model.CreateDocumentResponse;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class Client {

    private RestHighLevelClient client;

    private Client(RestHighLevelClient client) {
        this.client = client;
    }

    public static Client newClient(String host, int port) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)
                )
        );
        return new Client(client);
    }

    public void close() throws IOException {
        client.close();
    }

    public CreateDocumentResponse createDocument(String indexName, Map<String, Object> metadata, Optional<String> documentId )
            throws IllegalArgumentException, IOException {
        if (null == indexName || indexName.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (null == metadata) {
            throw new IllegalArgumentException();
        }

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

}
