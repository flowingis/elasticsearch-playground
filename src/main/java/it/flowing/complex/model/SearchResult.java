package it.flowing.complex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.List;

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
    private float maxScore;
    private TotalHits.Relation hitsRelation;

    // Hits
    private List<SearchHit> hits;

    public static SearchResult fromSearchResponse(SearchResponse searchResponse) {
        return (new SearchResult())
                .withStatus(searchResponse.status())
                .withTook(searchResponse.getTook())
                .withTerminatedEarly(searchResponse.isTerminatedEarly())
                .withTimedOut(searchResponse.isTimedOut())
                .withTotalShards(searchResponse.getTotalShards())
                .withSuccessfulShards(searchResponse.getTotalShards())
                .withFailedShards(searchResponse.getFailedShards())
                .withNumHits(searchResponse.getHits().getTotalHits().value)
                .withMaxScore(searchResponse.getHits().getMaxScore())
                .withHitsRelation(searchResponse.getHits().getTotalHits().relation)
                .withHits(Arrays.asList(searchResponse.getHits().getHits()));
    }

}
