package it.flowing.service;

import org.jboss.arquillian.container.test.api.Deployment;
import javax.inject.Inject;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
public class ConnectionTest {

    @Inject
    private Connection connection;

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false, "it.flowing.service")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test(expected = IllegalArgumentException.class)
    public void ShouldThrowErrorIfNullHostProvided() {
        ElasticSearchClient client = connection.open(null, 5601);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ShouldThrowErrorIfEmptyHostProvided() {
        ElasticSearchClient client = connection.open(null, 5601);
    }

    @Test
    public void ShouldOpenConnection() {
        ElasticSearchClient client = connection.open("localhost", 5601);
        assertNotNull(client);
    }

    @Test
    public void ShouldCloseConnection() {
        try {
            ElasticSearchClient client = connection.open("localhost", 5601);
            connection.close(client);
        } catch (IOException e) {
            fail();
        }
    }

}
