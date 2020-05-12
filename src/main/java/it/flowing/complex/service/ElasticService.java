package it.flowing.complex.service;

import com.google.common.base.Preconditions;
import lombok.NoArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

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
        createClient();
    }

    private void createClient() {
        Preconditions.checkNotNull(serverConfiguration);

        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(serverConfiguration.getHost(), serverConfiguration.getPort())
                )
        );
    }

    public void terminate() throws IOException {
        if (null != client) {
            client.close();
        }
    }

    public SearchResult search(String indexName, QueryBuilder queryBuilder) {
        checkSearchPreconditions(indexName, queryBuilder);

        return null;
    }

    private void checkSearchPreconditions(String indexName, QueryBuilder queryBuilder) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
        Preconditions.checkNotNull(queryBuilder);
    }

}
