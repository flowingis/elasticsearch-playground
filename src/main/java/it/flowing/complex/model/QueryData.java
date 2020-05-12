package it.flowing.complex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@With
@NoArgsConstructor
@AllArgsConstructor
@Data
public class QueryData {
    private SearchType searchType = SearchType.MATCH_ALL_QUERY;
    private Optional<Integer> from = Optional.empty();
    private Optional<Integer> size = Optional.empty();
    private Optional<TimeValue> timeout = Optional.empty();
    private List<SortBuilder<?>> sortingCriteria = new ArrayList<>();
    private Optional<String[]> includeFields = Optional.empty();
    private Optional<String[]> excludeFields = Optional.empty();
}
