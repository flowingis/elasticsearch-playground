package it.flowing.raw.service;

import it.flowing.raw.model.CreateDocumentResponse;
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

    public static final String HOST = "localhost";
    public static final int PORT = 9200;

    @Inject
    private RawConnection connection;

    private RawClient client;

    @Before
    public void init() {
        client = connection.open(HOST, PORT);
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

    @Test(expected = IllegalArgumentException.class)
    public void CreateDocumentShouldThrowErrorIfNullIndexProvided() throws IOException {
        client.createDocument(null, null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void CreateDocumentShouldThrowErrorIfEmptyIndexProvided() throws IOException {
        client.createDocument("", null, Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void CreateDocumentShouldThrowErrorIfNullMetadataProvided() throws IOException {
        client.createDocument("dummyIndex", null, Optional.empty());
    }

    @Test
    public void CreateDocumentShouldReturnAValidIndexRequestObject() {
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
    public void CreateDocumentShouldReturnTheIdProvided() {
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

//    @Test(expected = IllegalArgumentException.class)
//    public void GetDocumentByIdShouldThrowErrorIfNullIndexProvided() {
//        Document document = client.getDocumentById(null, id, Optional.of(options));
//    }

//    @Test(expected = IllegalArgumentException.class)
//    public void GetDocumentShouldThrowErrorIfEmptyIndexProvided() {
//        Document document = client.getDocumentById(null, id, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void GetDocumentShouldThrowErrorIfNullIdProvided() {
//        Document document = client.getDocumentById("javatest", null, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void GetDocumentShouldThrowErrorIfEmptyIdProvided() {
//        Document document = client.getDocumentById("javatest", "", Optional.of(options));
//    }
//
//    @Test
//    public void GetDocumentShouldReturnTheMetadataIfIdExists() {
//        try {
//            Document document = client.getDocumentById("javatest", id, Optional.of(options));
//
//            assertNotNull(document);
//        } catch (IOException e) {
//            fail();
//        }
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void UpdateDocumentByIdShouldThrowErrorIfEmptyIndexProvided() {
//        Document document = client.getDocumentById(null, id, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void UpdateDocumentByIdShouldThrowErrorIfNullIdProvided() {
//        Document document = client.getDocumentById("javatest", null, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void UpdateDocumentByIdShouldThrowErrorIfEmptyIdProvided() {
//        Document document = client.getDocumentById("javatest", "", Optional.of(options));
//    }
//
//    @Test
//    public void UpdateDocumentByIdShouldReturnTheMetadataIfIdExists() {
//        try {
//            Document document = client.getDocumentById("javatest", id, Optional.of(options));
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
//        Document document = client.getDocumentById("javatest", null, Optional.of(options));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void DeleteDocumentByIdShouldThrowErrorIfEmptyIdProvided() {
//        Document document = client.getDocumentById("javatest", "", Optional.of(options));
//    }
//
//    @Test
//    public void DeleteDocumentByIdShouldReturnTheMetadataIfIdExists() {
//        try {
//            Document document = client.getDocumentById("javatest", id, Optional.of(options));
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
