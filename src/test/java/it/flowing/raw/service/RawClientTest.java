package it.flowing.raw.service;

import com.github.javafaker.Faker;
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

    public static final String WRONG_VALUE = "asdf1234";

    private static boolean setUpIsDone = false;

    @Inject
    private RawConnection connection;

    @Inject
    private RawConfiguration rawConfiguration;

    private RawClient client;

    @Before
    public void before() {
        client = connection.open(rawConfiguration.getElasticSearchHost(), rawConfiguration.getElasticSearchPort());

        if (setUpIsDone) {
            return;
        }

        try {
            RawClient client = connection.open(rawConfiguration.getElasticSearchHost(), rawConfiguration.getElasticSearchPort());
            client.deleteIndex(rawConfiguration.getIndexNameTest(), Optional.empty());
            popolateIndexWithFakeData();
            connection.close(client);
        } catch (IOException e) {
            e.printStackTrace();
        }

        setUpIsDone = true;
    }

    private void popolateIndexWithFakeData() throws IOException {
        client.createDocument(rawConfiguration.getIndexNameTest(),
                getDummyDataForCreateDocument(),
                Optional.of(rawConfiguration.getUuidGetDocumentTest()));

        client.createDocument(rawConfiguration.getIndexNameTest(),
                getDummyDataForCreateDocument(),
                Optional.of(rawConfiguration.getUuidDeleteDocumentTest()));

        client.createDocument(rawConfiguration.getIndexNameDeleteTest(),
                getDummyDataForCreateDocument(),
                Optional.of(rawConfiguration.getUuidDeleteDocumentTest()));
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
    public void DeleteIndexShouldThrowErrorIfNullIndexProvided() throws Exception {
        client.deleteIndex(null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void DeleteIndexShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        client.deleteIndex("", Optional.empty());
    }

    @Test
    public void DeleteIndexShouldReturnTrueIfIndexExists() throws Exception {
        assertTrue(client.deleteIndex(rawConfiguration.getIndexNameDeleteTest(), Optional.empty()));
    }

    @Test
    public void DeleteIndexShouldReturnFalseIfIndexNotExists() throws Exception {
        assertFalse(client.deleteIndex(WRONG_VALUE, Optional.empty()));
    }

    @Test(expected = NullPointerException.class)
    public void CreateDocumentShouldThrowErrorIfNullIndexProvided() throws Exception {
        client.createDocument(null, null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void CreateDocumentShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        client.createDocument("", null, Optional.empty());
    }

    @Test(expected = NullPointerException.class)
    public void CreateDocumentShouldThrowErrorIfNullMetadataProvided() throws Exception {
        client.createDocument("dummyIndex", null, Optional.empty());
    }

    @Test
    public void CreateDocumentShouldReturnAValidIndexRequestObject() throws Exception {
        Map<String, Object> metadata = getDummyDataForCreateDocument();
        CreateDocumentResponse response = null;
        response = client.createDocument(rawConfiguration.getIndexNameTest(), metadata, Optional.empty());

        assertNotNull(response);
        assertEquals(RestStatus.CREATED, response.getStatus());
    }

    @Test
    public void CreateDocumentShouldReturnTheIdProvided() throws Exception {
        Map<String, Object> metadata = getDummyDataForCreateDocument();
        CreateDocumentResponse response = null;
        String uuid = UUID.randomUUID().toString();
        response = client.createDocument(rawConfiguration.getIndexNameTest(), metadata, Optional.of(uuid));

        assertNotNull(response);
        assertEquals(RestStatus.CREATED, response.getStatus());
        assertEquals(uuid, response.getId());
    }

    @Test(expected = NullPointerException.class)
    public void GetDocumentShouldThrowErrorIfNullIndexProvided() throws Exception {
        Document document = client.getDocument(null, rawConfiguration.getUuidGetDocumentTest(), Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void GetDocumentShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        Document document = client.getDocument("", rawConfiguration.getUuidGetDocumentTest(), Optional.empty());
    }

    @Test(expected = NullPointerException.class)
    public void GetDocumentShouldThrowErrorIfNullIdProvided() throws Exception {
        Document document = client.getDocument(rawConfiguration.getIndexNameTest(), null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void GetDocumentShouldThrowErrorIfEmptyIdProvided() throws Exception {
        Document document = client.getDocument(rawConfiguration.getIndexNameTest(), "", Optional.empty());
    }

    @Test
    public void GetDocumentWithExistingIdShouldReturnAValidDocumentWithMetadata() throws Exception {
        Document document = client.getDocument(
                    rawConfiguration.getIndexNameTest(),
                    rawConfiguration.getUuidGetDocumentTest(),
                    Optional.empty());

        assertNotNull(document);
    }

    @Test
    public void GetDocumentWithNonExistingShouldReturnNullDocument() throws Exception {
        Document document = client.getDocument(
                    rawConfiguration.getIndexNameTest(),
                    "asdf1234",
                    Optional.empty());

        assertNull(document);
    }

    @Test(expected = NullPointerException.class)
    public void ExistsDocumentShouldThrowErrorIfNullIndexProvided() throws Exception {
        boolean exists = client.existsDocument(null, rawConfiguration.getUuidGetDocumentTest());
    }

    @Test(expected = IllegalArgumentException.class)
    public void ExistsDocumentShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        boolean exists = client.existsDocument("", rawConfiguration.getUuidGetDocumentTest());
    }

    @Test(expected = NullPointerException.class)
    public void ExistsDocumentShouldThrowErrorIfNullIdProvided() throws Exception {
        boolean exists = client.existsDocument(rawConfiguration.getIndexNameTest(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ExistsDocumentShouldThrowErrorIfEmptyIdProvided() throws Exception {
        boolean exists = client.existsDocument(rawConfiguration.getIndexNameTest(), "");
    }

    @Test
    public void ExistsDocumentWithExistingIdShouldReturnTrue() throws Exception {
        boolean exists = client.existsDocument(
                rawConfiguration.getIndexNameTest(),
                rawConfiguration.getUuidGetDocumentTest());
        assertTrue(exists);
    }

    @Test
    public void ExistsDocumentWithNonExistingIdShouldReturnFalse() throws Exception {
        boolean exists = client.existsDocument(
                rawConfiguration.getIndexNameTest(),
                "asdf1234");
        assertFalse(exists);
    }

    @Test(expected = NullPointerException.class)
    public void UpdateDocumentShouldThrowErrorIfNullIndexProvided() throws Exception {
        UpdateDocumentResponse updateDocumentResponse = client.updateDocument(null,
                rawConfiguration.getUuidGetDocumentTest(),
                null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void UpdateDocumentShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        UpdateDocumentResponse updateDocumentResponse = client.updateDocument("",
                rawConfiguration.getUuidGetDocumentTest(),
                null, Optional.empty());
    }

    @Test(expected = NullPointerException.class)
    public void UpdateDocumentShouldThrowErrorIfNullIdProvided() throws Exception {
        UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                null,
                null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void UpdateDocumentShouldThrowErrorIfEmptyIdProvided() throws Exception {
        UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                "",
                null, Optional.empty());
    }

    @Test(expected = NullPointerException.class)
    public void UpdateDocumentShouldThrowErrorIfNullMetadataProvided() throws Exception {
        UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                rawConfiguration.getUuidGetDocumentTest(),
                null, Optional.empty());
    }

    @Test
    public void UpdateDocumentShouldChangeMetadataNome() throws Exception {
        UpdateDocumentResponse updateDocumentResponse = null;
        String newName = "Giulio";

        Map<String, Object> metadataToUpdate = new HashMap<>();
        metadataToUpdate.put("nome", newName);
        updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                rawConfiguration.getUuidGetDocumentTest(),
                metadataToUpdate,
                Optional.empty());

        Document document = client.getDocument(rawConfiguration.getIndexNameTest(),
                rawConfiguration.getUuidGetDocumentTest(),
                Optional.empty());

        assertNotNull(updateDocumentResponse);
        assertEquals(newName, document.getSource().get("nome"));
    }

    @Test
    public void UpdateDocumentWithIdNotFoundShouldReturnNull() throws Exception {
        Map<String, Object> metadataToUpdate = new HashMap<>();
        metadataToUpdate.put("nome", "Giulio");
        UpdateDocumentResponse updateDocumentResponse = client.updateDocument(rawConfiguration.getIndexNameTest(),
                "asdf1234",
                metadataToUpdate,
                Optional.empty());

        assertNull(updateDocumentResponse);
    }

    @Test(expected = NullPointerException.class)
    public void DeleteDocumentShouldThrowErrorIfNullIndexProvided() throws Exception {
        DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(null,
                rawConfiguration.getUuidDeleteDocumentTest(),
                Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void DeleteDocumentShouldThrowErrorIfEmptyIndexProvided() throws Exception {
        DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument("",
                rawConfiguration.getUuidDeleteDocumentTest(),
                Optional.empty());
    }

    @Test(expected = NullPointerException.class)
    public void DeleteDocumentShouldThrowErrorIfNullIdProvided() throws Exception {
        DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                null,
                Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void DeleteDocumentShouldThrowErrorIfEmptyIdProvided() throws Exception {
        DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                "",
                Optional.empty());
    }

    @Test
    public void DeleteDocumentWithValidIdShouldRemoveDocumentFromIndex() throws Exception {
        DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                rawConfiguration.getUuidDeleteDocumentTest(),
                Optional.empty());

        Document document = client.getDocument(rawConfiguration.getIndexNameTest(),
                rawConfiguration.getUuidDeleteDocumentTest(),
                Optional.empty());

        assertNotNull(deleteDocumentResponse);
        assertNull(document);
    }

    @Test
    public void DeleteDocumentWithNonExistingIdShouldReturnNull() throws Exception {
        DeleteDocumentResponse deleteDocumentResponse = client.deleteDocument(rawConfiguration.getIndexNameTest(),
                "asdf1234",
                Optional.empty());

        assertNull(deleteDocumentResponse);
    }

    private Map<String, Object> getDummyDataForCreateDocument() {
        Faker faker = new Faker();

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("nome", faker.name().firstName());
        metadata.put("cognome", faker.name().lastName());
        return metadata;
    }

}
