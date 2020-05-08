package it.flowing.raw.service;

import lombok.Getter;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Getter
public class RawConfiguration {
    private String uuidGetDocumentTest;
    private String elasticSearchHost;
    private int elasticSearchPort;
    private String indexNameTest;

    public RawConfiguration() {
        // TODO: Leggere da file di configurazione
        uuidGetDocumentTest = "a4135c2b-455f-4832-b59e-8938239a5424";
        elasticSearchHost = "localhost";
        elasticSearchPort = 9200;
        indexNameTest = "javatest";
    }

}
