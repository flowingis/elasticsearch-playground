package it.flowing.raw.service;

import it.flowing.raw.model.CreateDocumentResponse;
import it.flowing.raw.model.DeleteDocumentResponse;
import it.flowing.raw.model.Document;
import it.flowing.raw.model.UpdateDocumentResponse;
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

    @Test(expected = NullPointerException.class)
    public void UpdateDocumentShouldThrowErrorIfNullIndexProvided() {
        try {
            UpdateDocumentResponse updateDocumentResponse = client.updateDocument(null,
                    rawConfiguration.getUuidGetDocumentTest(),
                    null, Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void UpdateDocumentShouldThrowErrorIfEmptyIndexProvided() {
        try {
            UpdateDocumentResponse updateDocumentResponse = client.updateDocument("",
                    rawConfiguration.getUuidGetDocumentTest(),
                    null, Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void UpdateDocumentShouldThrowErrorIfNullIdProvided() {
        try {
            UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                    null,
                    null, Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void UpdateDocumentShouldThrowErrorIfEmptyIdProvided() {
        try {
            UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                    "",
                    null, Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void UpdateDocumentShouldThrowErrorIfNullMetadataProvided() {
        try {
            UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidGetDocumentTest(),
                    null, Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void UpdateDocumentShouldChangeMetadataNome() {
        UpdateDocumentResponse updateDocumentResponse = null;
        Document document = null;
        String newName = "Giulio";
        try {
            Map<String, Object> metadataToUpdate = new HashMap<>();
            metadataToUpdate.put("nome", newName);
            updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidGetDocumentTest(),
                    metadataToUpdate,
                    Optional.empty());

            document = client.getDocument(rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidGetDocumentTest(),
                    Optional.empty());

        } catch (IOException e) {
            fail();
        }

        assertNotNull(updateDocumentResponse);
        assertEquals(newName, document.getSource().get("nome"));
    }

    @Test
    public void UpdateDocumentWithIdNotFoundShouldReturnNull() {
        UpdateDocumentResponse updateDocumentResponse = null;
        try {
            Map<String, Object> metadataToUpdate = new HashMap<>();
            metadataToUpdate.put("nome", "Giulio");
            updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                    "asdf1234",
                    metadataToUpdate,
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }

        assertNull(updateDocumentResponse);
    }

    @Test(expected = NullPointerException.class)
    public void DeleteDocumentShouldThrowErrorIfNullIndexProvided() {
        try {
            DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(null,
                    rawConfiguration.getUuidDeleteDocumentTest(),
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void DeleteDocumentShouldThrowErrorIfEmptyIndexProvided() {
        try {
            DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument("",
                    rawConfiguration.getUuidDeleteDocumentTest(),
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void DeleteDocumentShouldThrowErrorIfNullIdProvided() {
        try {
            DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                    null,
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void DeleteDocumentShouldThrowErrorIfEmptyIdProvided() {
        try {
            DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                    "",
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void DeleteDocumentShouldRemoveDocumentFromIndex() {
        DeleteDocumentResponse deleteDocumentResponse = null;
        Document document = null;
        try {
            deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidDeleteDocumentTest(),
                    Optional.empty());

            document = client.getDocument(rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidDeleteDocumentTest(),
                    Optional.empty());

        } catch (IOException e) {
            fail();
        }

        assertNotNull(deleteDocumentResponse);
        assertNull(document);
    }

    @Test
    public void DeleteDocumentWithNonExistingIdShouldReturnNull() {
        DeleteDocumentResponse deleteDocumentResponse = null;
        try {
            deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidDeleteDocumentTest(),
                    Optional.empty());
        } catch (IOException e) {
            fail();
        }

        assertNull(deleteDocumentResponse);
    }
    
    private Map<String, Object> getDummyDataForCreateDocument() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("nome", "Marco");
        metadata.put("cognome", "Neri");
        return metadata;
    }

}
