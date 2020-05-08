package it.flowing.raw.service;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(Arquillian.class)
public class RawConnectionTest {

    @Inject
    private RawConnection connection;

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false,
                        "it.flowing.raw.service",
                        "it.flowing.raw.model")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test(expected = IllegalArgumentException.class)
    public void ShouldThrowErrorIfNullHostProvided() {
        RawClient client = connection.open(null, 5601);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ShouldThrowErrorIfEmptyHostProvided() {
        RawClient client = connection.open(null, 5601);
    }

    @Test
    public void ShouldOpenConnection() {
        RawClient client = connection.open("localhost", 5601);
        assertNotNull(client);
    }

    @Test
    public void ShouldCloseConnection() {
        try {
            RawClient client = connection.open("localhost", 5601);
            connection.close(client);
        } catch (IOException e) {
            fail();
        }
    }

}
