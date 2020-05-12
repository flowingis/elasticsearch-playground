package it.flowing.complex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

@With
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SearchResult {

    // Request Execution
    private RestStatus status;
    private TimeValue took;
    private Boolean terminatedEarly;
    private Boolean timedOut;

    // Shards Execution
    private int totalShards;
    private int successfulShards;
    private int failedShards;

    // SearchHits
    private long numHits;

    public static SearchResult fromSearchResponse(SearchResponse searchResponse) {
        return (new SearchResult())
                .withStatus(searchResponse.status())
                .withTook(searchResponse.getTook())
                .withTerminatedEarly(searchResponse.isTerminatedEarly())
                .withTimedOut(searchResponse.isTimedOut())
                .withTotalShards(searchResponse.getTotalShards())
                .withSuccessfulShards(searchResponse.getTotalShards())
                .withFailedShards(searchResponse.getFailedShards())
                .withNumHits(searchResponse.getHits().getTotalHits().value);
    }

}
