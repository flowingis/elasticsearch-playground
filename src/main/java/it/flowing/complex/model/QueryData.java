package it.flowing.complex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

@With
@NoArgsConstructor
@AllArgsConstructor
@Data
public class QueryData {
    private SearchType searchType;
}
