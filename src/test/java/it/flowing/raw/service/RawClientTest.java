package it.flowing.raw.service;

import it.flowing.raw.model.CreateDocumentResponse;
import it.flowing.raw.model.Document;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Arquillian.class)
public class RawClientTest {

    @Inject
    private RawConnection connection;

    @Inject
    private RawConfiguration rawConfiguration;

    private RawClient client;

    @Before
    public void init() {
        client = connection.open(rawConfiguration.getElasticSearchHost(), rawConfiguration.getElasticSearchPort());
    }

    @After
    public void terminate() throws IOException {
        connection.close(client);
    }

    @Deployment
    public static WebArchive createDeployment() {
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(false,
                        "it.flowing.raw.service",
                        "it.flowing.raw.model")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test(expected = NullPointerException.class)
    public void CreateDocumentShouldThrowErrorIfNullIndexProvided() throws IOException {
        client.createDocument(null, null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void CreateDocumentShouldThrowErrorIfEmptyIndexProvided() throws IOException {
        client.createDocument("", null, Optional.empty());
    }

    @Test(expected = NullPointerException.class)
    public void CreateDocumentShouldThrowErrorIfNullMetadataProvided() throws IOException {
        client.createDocument("dummyIndex", null, Optional.empty());
    }

    @Test
    public void CreateDocumentShouldReturnAValidIndexRequestObject() {
        try {
            Map<String, Object> metadata = getDummyDataForCreateDocument();
            CreateDocumentResponse response = null;
            response = client.createDocument(rawConfiguration.getIndexNameTest(), metadata, Optional.empty());

            assertNotNull(response);
            assertEquals(RestStatus.CREATED, response.getStatus());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void CreateDocumentShouldReturnTheIdProvided() {
        try {
            Map<String, Object> metadata = getDummyDataForCreateDocument();
            CreateDocumentResponse response = null;
            String uuid = UUID.randomUUID().toString();
            response = client.createDocument(rawConfiguration.getIndexNameTest(), metadata, Optional.of(uuid));

            assertNotNull(response);
            assertEquals(RestStatus.CREATED, response.getStatus());
            assertEquals(uuid, response.getId());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void GetDocumentShouldThrowErrorIfNullIndexProvided() {
        try {
            Document document = client.getDocument(null, rawConfiguration.getUuidGetDocumentTest(), Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void GetDocumentShouldThrowErrorIfEmptyIndexProvided() {
        try {
            Document document = client.getDocument("", rawConfiguration.getUuidGetDocumentTest(), Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void GetDocumentShouldThrowErrorIfNullIdProvided() {
        try {
            Document document = client.getDocument(rawConfiguration.getIndexNameTest(), null, Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void GetDocumentShouldThrowErrorIfEmptyIdProvided() {
        try {
            Document document = client.getDocument(rawConfiguration.getIndexNameTest(), "", Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void GetDocumentWithExistingIdShouldReturnAValidDocumentWithMetadata() {
        Document document = null;
        try {
            document = client.getDocument(
                    rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidGetDocumentTest(),
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }

        assertNotNull(document);
        assertEquals("Marco", document.getSource().get("nome"));
        assertEquals("Neri", document.getSource().get("cognome"));
    }

    @Test
    public void GetDocumentWithNonExistingShouldReturnNullDocument() {
        Document document = null;
        try {
            document = client.getDocument(
                    rawConfiguration.getIndexNameTest(),
                    "asdf1234",
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }

        assertNull(document);
    }

    @Test(expected = NullPointerException.class)
    public void ExistsDocumentShouldThrowErrorIfNullIndexProvided() {
        try {
            boolean exists = client.existsDocument(null, rawConfiguration.getUuidGetDocumentTest());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void ExistsDocumentShouldThrowErrorIfEmptyIndexProvided() {
        try {
            boolean exists = client.existsDocument("", rawConfiguration.getUuidGetDocumentTest());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void ExistsDocumentShouldThrowErrorIfNullIdProvided() {
        try {
            boolean exists = client.existsDocument(rawConfiguration.getIndexNameTest(), null);
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void ExistsDocumentShouldThrowErrorIfEmptyIdProvided() {
        try {
            boolean exists = client.existsDocument(rawConfiguration.getIndexNameTest(), "");
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void ExistsDocumentWithExistingIdShouldReturnTrue() {
        try {
            boolean exists = client.existsDocument(
                    rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidGetDocumentTest());
            assertTrue(exists);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void ExistsDocumentWithNonExistingIdShouldReturnFalse() {
        try {
            boolean exists = client.existsDocument(
                    rawConfiguration.getIndexNameTest(),
                    "asdf1234");
            assertFalse(exists);
        } catch (IOException e) {
            fail();
        }
    }

//    @Test(expected = IllegalArgumentException.class)
//    public void UpdateDocumentByIdShouldThrowErrorIfEmptyIndexProvided() {
//        Document document = client.getDocumentById(null, id, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void UpdateDocumentByIdShouldThrowErrorIfNullIdProvided() {
//        Document document = client.getDocumentById(rawConfiguration.getIndexNameTest(), null, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void UpdateDocumentByIdShouldThrowErrorIfEmptyIdProvided() {
//        Document document = client.getDocumentById(rawConfiguration.getIndexNameTest(), "", Optional.of(options));
//    }
//
//    @Test
//    public void UpdateDocumentByIdShouldReturnTheMetadataIfIdExists() {
//        try {
//            Document document = client.getDocumentById(rawConfiguration.getIndexNameTest(), id, Optional.of(options));
//
//            assertNotNull(document);
//        } catch (IOException e) {
//            fail();
//        }
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void DeleteDocumentByIdShouldThrowErrorIfEmptyIndexProvided() {
//        Document document = client.getDocumentById(null, id, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void DeleteDocumentByIdShouldThrowErrorIfNullIdProvided() {
//        Document document = client.getDocumentById(rawConfiguration.getIndexNameTest(), null, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void DeleteDocumentByIdShouldThrowErrorIfEmptyIdProvided() {
//        Document document = client.getDocumentById(rawConfiguration.getIndexNameTest(), "", Optional.of(options));
//    }
//
//    @Test
//    public void DeleteDocumentByIdShouldReturnTheMetadataIfIdExists() {
//        try {
//            Document document = client.getDocumentById(rawConfiguration.getIndexNameTest(), id, Optional.of(options));
//
//            assertNotNull(document);
//        } catch (IOException e) {
//            fail();
//        }
//    }

    private Map<String, Object> getDummyDataForCreateDocument() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("nome", "Marco");
        metadata.put("cognome", "Neri");
        return metadata;
    }

}
