package it.flowing.complex.model;

import lombok.Builder;
import lombok.Getter;
import org.elasticsearch.rest.RestStatus;

@Getter
@Builder
public class DeleteDocumentResponse {
    private RestStatus status;
    private String id;
}
