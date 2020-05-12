package it.flowing.complex.service;

import lombok.Data;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Data
public class ServerConfiguration {
    private String host;
    private int port;

    public ServerConfiguration() {
        // TODO: Leggere da un file di configurazione
        host = "localhost";
        port = 9200;
    }
}
