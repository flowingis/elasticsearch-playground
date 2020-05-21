package it.flowing.complex.service.configuration;

import lombok.Data;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Data
public class ServerConfiguration {
    private String host;
    private int port;
    private String searchIndex;

    public ServerConfiguration() {
        // TODO: Leggere da un file di configurazione
        host = "localhost";
        port = 9200;
        searchIndex = "kibana_sample_data_ecommerce";
    }
}
