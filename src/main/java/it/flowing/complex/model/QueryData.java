package it.flowing.complex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

import java.util.Optional;

@With
@NoArgsConstructor
@AllArgsConstructor
@Data
public class QueryData {
    private SearchType searchType = SearchType.MATCH_ALL_QUERY;
    private Optional<Integer> from = Optional.empty();
    private Optional<Integer> size = Optional.empty();
}
