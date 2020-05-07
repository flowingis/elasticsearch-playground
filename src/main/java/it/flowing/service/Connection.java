package it.flowing.service;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

@ApplicationScoped
public class Connection {

    private static final String PROTOCOL = "http";
    private static final int DEFAULT_PORT = 9200;

    public ElasticSearchClient open(String host, int port) throws IllegalArgumentException {
        checkHost(host);
        port = checkPort(port);

        return ElasticSearchClient.newClient(host, port);
    }

    private int checkPort(int port) {
        if (0 == port) {
            port = DEFAULT_PORT;
        }
        return port;
    }

    private void checkHost(String host) {
        if (null == host || host.isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    public void close(ElasticSearchClient client) throws IOException {
        if (null != client) {
            client.close();
        }
    }

}
