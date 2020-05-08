package it.flowing.raw.model;

import lombok.Builder;
import lombok.Getter;
import org.elasticsearch.common.document.DocumentField;

import java.util.Map;

@Getter
@Builder
public class Document {
    private Map<String, DocumentField> fields;
    private Map<String, Object> source;
}
