package it.flowing.complex.service;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class SearcherTest {

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false,
                        "it.flowing.complex.service")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test(expected = NullPointerException.class)
    public void ShouldThrowErrorIfNullIndexNameProvided() {
        Searcher searcher = new Searcher();
        searcher.search(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ShouldThrowErrorIfEmptyIndexNameProvided() {
        Searcher searcher = new Searcher();
        searcher.search("");
    }

}
