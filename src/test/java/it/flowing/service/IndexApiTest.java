package it.flowing.service;

import model.CreateDocumentResponse;
import org.elasticsearch.rest.RestStatus;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
public class IndexApiTest {

    public static final String HOST = "localhost";
    public static final int PORT = 9200;

    @Inject
    private Connection connection;

    private Client client;

    @Before
    public void init() {
        client = connection.open(HOST, PORT);
    }

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false, "it.flowing.service")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test(expected = IllegalArgumentException.class)
    public void IndexRequestShouldThrowErrorIfNullIndexProvided() throws IOException {
        client.createDocument(null, null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void IndexRequestShouldThrowErrorIfEmptyIndexProvided() throws IOException {
        client.createDocument("", null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void IndexRequestShouldThrowErrorIfNullMetadataProvided() throws IOException {
        client.createDocument("dummyIndex", null, Optional.empty());
    }

    @Test
    public void IndexRequestShouldReturnAValidIndexRequestObject() {
        try {
            Map<String, Object> metadata = getDummyDataForCreateDocument();
            CreateDocumentResponse response = null;
            response = client.createDocument("javatest", metadata, Optional.empty());

            assertNotNull(response);
            assertEquals(RestStatus.CREATED, response.getStatus());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void IndexRequestShouldReturnTheIdProvided() {
        try {
            Map<String, Object> metadata = getDummyDataForCreateDocument();
            CreateDocumentResponse response = null;
            String uuid = UUID.randomUUID().toString();
            response = client.createDocument("javatest", metadata, Optional.of(uuid));

            assertNotNull(response);
            assertEquals(RestStatus.CREATED, response.getStatus());
            assertEquals(uuid, response.getId());
        } catch (IOException e) {
            fail();
        }
    }

    private Map<String, Object> getDummyDataForCreateDocument() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("nome", "Marco");
        metadata.put("cognome", "Neri");
        return metadata;
    }

}
