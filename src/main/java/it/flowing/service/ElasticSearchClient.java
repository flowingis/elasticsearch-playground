package it.flowing.service;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticSearchClient {

    private RestHighLevelClient client;

    private ElasticSearchClient(RestHighLevelClient client) {
        this.client = client;
    }

    public static ElasticSearchClient newClient(String host, int port) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)
                )
        );
        return new ElasticSearchClient(client);
    }

    public void close() throws IOException {
        client.close();
    }

}
