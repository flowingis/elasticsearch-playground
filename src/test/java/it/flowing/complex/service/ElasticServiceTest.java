package it.flowing.complex.service;

import it.flowing.complex.model.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
public class ElasticServiceTest {

    @Inject
    private ElasticService elasticService;

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false,
                        "it.flowing.complex.service",
                        "it.flowing.complex.model")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Before
    public void before() {
        elasticService.openConnection();
    }

    @After
    public void terminate() throws IOException {
        elasticService.closeConnection();
    }

    @Test(expected = NullPointerException.class)
    public void SearchShouldThrowErrorIfNullQueryBuilderProvided() throws Exception {
        elasticService.search(null);
    }

    @Test
    public void SearchShouldReturnAValidSearchResult() throws Exception {
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY);

        SearchResult searchResult = elasticService.search(queryData);

        // About SearchResult
        assertNotNull(searchResult);

        // About Search Execution
        assertEquals(RestStatus.OK, searchResult.getStatus());
        assertFalse(searchResult.getTimedOut());
        assertNull(searchResult.getTerminatedEarly());
        assertNotEquals(0L, searchResult.getTook().duration());

        // About Shards Execution
        assertNotEquals(0, searchResult.getTotalShards());
        assertNotEquals(0, searchResult.getSuccessfulShards());
        assertEquals(0, searchResult.getFailedShards());

        // Hits
        assertEquals(4675L, searchResult.getNumHits());
        assertEquals(1.0f, searchResult.getMaxScore(), 0.1f);
        assertEquals(TotalHits.Relation.EQUAL_TO, searchResult.getHitsRelation());
    }

    @Test
    public void SearchWithFromShouldReturnTheRightSubsetOfResult() throws Exception {
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withFrom(Optional.of(4670));

        SearchResult searchResult = elasticService.search(queryData);

        assertEquals(5L, searchResult.getHits().size());
    }

    @Test
    public void SearchWithSizeShouldReturnTheRightSubsetOfResult() throws Exception {
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withSize(Optional.of(5));

        SearchResult searchResult = elasticService.search(queryData);

        assertEquals(5L, searchResult.getHits().size());
    }

    @Test
    public void SearchWithPaginationShouldReturnTheRightSubsetOfResult() throws Exception {
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withFrom(Optional.of(1))
                .withSize(Optional.of(2));

        SearchResult searchResult = elasticService.search(queryData);

        assertEquals(2L, searchResult.getHits().size());
    }

    @Test
    public void SearchWithTimeoutShouldReturnTimedOutStatus() throws Exception {
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withTimeout(Optional.of(new TimeValue(5, TimeUnit.MINUTES)));

        SearchResult searchResult = elasticService.search(queryData);

        // TODO: Verificare: impostando il timeout ad un valore basso, se la query impiega di più, getTimedOut ritorna false
        assertFalse(searchResult.getTimedOut());
    }

    @Test
    public void SearchWithSortingCriteriaShouldReturnSortedResults() throws Exception {
        List<SortBuilder<?>> sortingCriteria = new ArrayList<>();
        sortingCriteria.add(new FieldSortBuilder("customer_id").order(SortOrder.ASC));
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withSortingCriteria(sortingCriteria)
                .withSize(Optional.of(1));

        SearchResult searchResult = elasticService.search(queryData);

        assertEquals(10, searchResult.getHits().get(0).getSourceAsMap().get("customer_id"));
    }

    @Test
    public void SearchWithExcludeAndIncludeFieldsShouldReturnOnlyTheRequestedFields() throws Exception {
        String[] excludeFields = new String[] { "customer_id" };
        String[] includeFields = new String[] { "email" };
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withExcludeFields(Optional.of(excludeFields))
                .withIncludeFields(Optional.of(includeFields))
                .withSize(Optional.of(1));

        SearchResult searchResult = elasticService.search(queryData);

        assertNull(searchResult.getHits().get(0).getSourceAsMap().get("customer_id"));
        assertNotNull(searchResult.getHits().get(0).getSourceAsMap().get("email"));
    }

    @Test
    public void SearchWithHighlighFieldShouldReturnHighlighedResult() throws Exception {
        List<Map<String, Object>> highlightFields = new ArrayList<>();
        highlightFields.add(
            new HashMap<String, Object>() {{
                put(HighlightFieldType.NAME.toString(), "email");
                put(HighlightFieldType.TYPE.toString(), "unified");
            }}
        );
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withHighlightFields(highlightFields)
                .withSize(Optional.of(1));

        SearchResult searchResult = elasticService.search(queryData);

        // TODO: Modificare assertion una volta definiti dei parametri di ricerca
        assertEquals(0, searchResult.getHits().get(0).getHighlightFields().size());
    }

    @Test
    public void SearchWithAggregationShouldReturnAggregateField() throws Exception {
        List<Map<String, Object>> aggregationInfo = new ArrayList<>();
        aggregationInfo.add(
            new HashMap<String, Object>() {{
                put(AggregationInfoFieldName.TERM.toString(), "by_customer_gender");
                put(AggregationInfoFieldName.FIELD.toString(), "customer_gender");
                put(AggregationInfoFieldName.AGGREGATION_TYPE.toString(), AggregationType.AVG);
                put(AggregationInfoFieldName.SUB_NAME.toString(), "average_day_of_week_i");
                put(AggregationInfoFieldName.SUB_FIELD.toString(), "day_of_week_i");
            }}
        );
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withAggregationInfo(aggregationInfo);

        SearchResult searchResult = elasticService.search(queryData);

        Terms byGenderAggregation = searchResult.getAggregations().get("by_customer_gender");

        Avg averageDayOfWeekMale = byGenderAggregation.getBucketByKey("MALE")
                .getAggregations()
                .get("average_day_of_week_i");

        Avg averageDayOfWeekFemale = byGenderAggregation.getBucketByKey("FEMALE")
                .getAggregations()
                .get("average_day_of_week_i");

        assertEquals(1, searchResult.getAggregations().asList().size());
        assertEquals(3.16, averageDayOfWeekMale.getValue(), 0.01);
        assertEquals(3.07, averageDayOfWeekFemale.getValue(), 0.01);
    }

    @Test
    public void SearchWithSuggestionShouldReturnSuggestedResult() throws Exception {
        List<Pair<String, String>> suggestions = new ArrayList<>();
        suggestions.add(new ImmutablePair<>("currency", "EUR"));
        QueryData queryData = (new QueryData())
                .withSearchType(SearchType.MATCH_ALL_QUERY)
                .withSuggestions(suggestions)
                .withSize(Optional.of(1));

        SearchResult searchResult = elasticService.search(queryData);

        assertEquals(1, searchResult.getSuggest().getSuggestion("suggest_currency").getEntries().size());
        assertEquals("EUR", searchResult.getSuggest().getSuggestion("suggest_currency").getEntries().get(0).getText().string());
    }

}
