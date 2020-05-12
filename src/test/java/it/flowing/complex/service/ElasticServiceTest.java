package it.flowing.complex.service;

import it.flowing.complex.model.QueryData;
import it.flowing.complex.model.SearchResult;
import it.flowing.complex.model.SearchType;
import org.elasticsearch.rest.RestStatus;
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
    }

}
