package it.flowing.complex.service;

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

@RunWith(Arquillian.class)
public class ElasticServiceTest {

    public static final String FAKE_INDEX = "fakeIndex";
    private static boolean setUpIsDone = false;

    @Inject
    private ElasticService elasticService;

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false,
                        "it.flowing.complex.service")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Before
    public void before() {
        if (setUpIsDone) {
            return;
        }

        setUpIsDone = true;
    }

    @After
    public void terminate() throws IOException {
        elasticService.terminate();
    }

    @Test(expected = NullPointerException.class)
    public void SearchShouldThrowErrorIfNullIndexProvided() throws Exception {
        elasticService.search(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void SearchShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        elasticService.search("", null);
    }

    @Test(expected = NullPointerException.class)
    public void SearchShouldThrowErrorIfNullQueryBuilderProvided() throws Exception {
        elasticService.search(FAKE_INDEX, null);
    }

}
