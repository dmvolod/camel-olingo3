/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.olingo3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.component.olingo3.api.Olingo3App;
import org.apache.camel.component.olingo3.api.Olingo3ResponseHandler;
import org.apache.camel.component.olingo3.api.impl.Olingo3AppImpl;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.olingo.client.api.edm.xml.Schema;
import org.apache.olingo.client.api.serialization.v3.ODataReader;
import org.apache.olingo.client.api.v3.ODataClient;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.domain.CommonODataProperty;
import org.apache.olingo.commons.api.domain.ODataCollectionValue;
import org.apache.olingo.commons.api.domain.ODataComplexValue;
import org.apache.olingo.commons.api.domain.ODataPrimitiveValue;
import org.apache.olingo.commons.api.domain.ODataServiceDocument;
import org.apache.olingo.commons.api.domain.ODataValue;
import org.apache.olingo.commons.api.domain.v3.ODataEntity;
import org.apache.olingo.commons.api.domain.v3.ODataEntitySet;
import org.apache.olingo.commons.api.domain.v3.ODataObjectFactory;
import org.apache.olingo.commons.api.domain.v3.ODataProperty;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.format.ODataFormat;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOptionKind;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Integration test for
 * {@link org.apache.camel.component.olingo3.Olingo3AppImpl.impl.Olingo3AppImpl} using the
 * sample OData 3.0 remote service published on
 * https://services.odata.org/V3/OData/OData.svc
 */
public class Olingo3AppAPITest {

    private static final Logger LOG = LoggerFactory.getLogger(Olingo3AppAPITest.class);
    private static final long TIMEOUT = 10;

    private static final String PERSONS = "Persons";
    
    private static final String TEST_PERSON = "Persons(1)";
    private static final String TEST_PRODUCT = "Products(2)";
    
    private static final String CATEGORIES = "Categories";
    private static final String TEST_CREATE_RESOURCE_CONTENT_ID = "1";
    private static final String TEST_UPDATE_RESOURCE_CONTENT_ID = "2";
    private static final String TEST_CREATE_KEY = "'lewisblack'";
    //private static final String TEST_CREATE_PEOPLE = PEOPLE + "(" + TEST_CREATE_KEY + ")";
    private static final String TEST_AIRPORT = "Airports('KSFO')";
    private static final String TEST_PERSON_SIMPLE_PROPERTY = TEST_PERSON + "/Name";
    private static final String TEST_AIRPORTS_COMPLEX_PROPERTY = TEST_AIRPORT + "/Location";
    private static final String TEST_PERSON_SIMPLE_PROPERTY_VALUE = TEST_PERSON_SIMPLE_PROPERTY + "/$value";
    private static final String COUNT_OPTION = "/$count";

    private static final String TEST_SERVICE_BASE_URL = "https://services.odata.org/V3/(S(readwrite))/OData/OData.svc/";
    private static final ContentType TEST_FORMAT = ContentType.APPLICATION_JSON;
    private static final String TEST_FORMAT_STRING = TEST_FORMAT.toString();
    
    private static final String TEST_ENTITY_CONTAINER_NAME = "DemoService";
    private static final String TEST_ENTITY_CONTAINER_NS = "ODataDemo";
    
    private static Olingo3App olingoApp;
    private static Edm edm;
    private final ODataClient odataClient = ODataClientFactory.getV3();
    private final ODataObjectFactory objFactory = odataClient.getObjectFactory();
    private final ODataReader reader = odataClient.getReader();

    @BeforeClass
    public static void beforeClass() throws Exception {
        setupClient();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (olingoApp != null) {
            olingoApp.close();
        }
    }

    protected static void setupClient() throws Exception {
    	String serviceUrl = getRealServiceUrl(TEST_SERVICE_BASE_URL);
        olingoApp = new Olingo3AppImpl(serviceUrl);
        olingoApp.setContentType(TEST_FORMAT_STRING);

        LOG.info("Read Edm ");
        final TestOlingo3ResponseHandler<Edm> responseHandler = new TestOlingo3ResponseHandler<>();
        
        olingoApp.read(null, Constants.METADATA, null, null, responseHandler);

        FullQualifiedName fqn = new FullQualifiedName(TEST_ENTITY_CONTAINER_NS, TEST_ENTITY_CONTAINER_NAME);
        edm = responseHandler.await();

        LOG.info("Read default EntityContainer:  {}", responseHandler.await().getEntityContainer(fqn).getName());
    }

    /*
     * Every request to the demo OData 3.0
     * (https://services.odata.org/V3/(S(readwrite))/OData/OData.svc/) generates unique
     * service URL with postfix like (S(tuivu3up5ygvjzo5fszvnwfv)) for each
     * session This method makes request to the base URL, return URL with
     * generated postfix and end URL
     */
    @SuppressWarnings("deprecation")
    protected static String getRealServiceUrl(String baseUrl) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(baseUrl);
        HttpContext httpContext = new BasicHttpContext();
        httpclient.execute(httpGet, httpContext);
        HttpUriRequest currentReq = (HttpUriRequest)httpContext.getAttribute(ExecutionContext.HTTP_REQUEST);
        HttpHost currentHost = (HttpHost)httpContext.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
        String currentUrl = (currentReq.getURI().isAbsolute()) ? currentReq.getURI().toString() : (currentHost.toURI() + currentReq.getURI());

        return currentUrl;
    }
    
    /*
    @Test
    public void testEntityDataModel() throws Exception {
    	assertNotNull(edm.getSchema(TEST_ENTITY_CONTAINER_NS).getEntityContainer().getName());
    }
    
    @Test
    public void testServiceDocument() throws Exception {
        final TestOlingo3ResponseHandler<ODataServiceDocument> responseHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.read(null, "", null, null, responseHandler);
        final ODataServiceDocument serviceDocument = responseHandler.await();

        final Map<String, URI> entitySets = serviceDocument.getEntitySets();
        assertEquals("Service Entity Sets", 7, entitySets.size());
        LOG.info("Service Document Entries:  {}", entitySets);
    }
    
    @Test
    public void testReadEntitySet() throws Exception {
        final TestOlingo3ResponseHandler<ODataEntitySet> responseHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.read(edm, PERSONS, null, null, responseHandler);

        final ODataEntitySet entitySet = responseHandler.await();
        assertNotNull(entitySet);
        assertEquals("Entity set count", 7, entitySet.getEntities().size());
        LOG.info("Entities:  {}", prettyPrint(entitySet));
    }


    @Test
    public void testReadUnparsedEntitySet() throws Exception {
        final TestOlingo3ResponseHandler<InputStream> responseHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.uread(edm, PERSONS, null, null, responseHandler);

        final InputStream rawEntitySet = responseHandler.await();
        assertNotNull("Data entity set", rawEntitySet);
        final ODataEntitySet entitySet = reader.readEntitySet(rawEntitySet, ODataFormat.fromContentType(TEST_FORMAT));
        assertEquals("Entity set count", 7, entitySet.getEntities().size());
        LOG.info("Entries:  {}", prettyPrint(entitySet));
    }


    @Test
    public void testReadEntity() throws Exception {
        final TestOlingo3ResponseHandler<ODataEntity> responseHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.read(edm, TEST_PERSON, null, null, responseHandler);
        ODataEntity entity = responseHandler.await();
        assertEquals("Jose Pavarotti", entity.getProperty("Name").getValue().toString());
        LOG.info("Single entity:  {}", prettyPrint(entity));

        responseHandler.reset();

        olingoApp.read(edm, TEST_PRODUCT, null, null, responseHandler);
        entity = responseHandler.await();
        assertEquals("20.9", entity.getProperty("Price").getValue().toString());
        LOG.info("Single Entry:  {}", prettyPrint(entity));
        
        responseHandler.reset();
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put(SystemQueryOptionKind.EXPAND.toString(), CATEGORIES);

        olingoApp.read(edm, TEST_PRODUCT, queryParams, null, responseHandler);
        ODataEntity entityExpanded = responseHandler.await();
        LOG.info("Single Product entity with expanded Categories relation:  {}", prettyPrint(entityExpanded));
    }

    @Test
    public void testReadUnparsedEntity() throws Exception {
        final TestOlingo3ResponseHandler<InputStream> responseHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.uread(edm, TEST_PERSON, null, null, responseHandler);
        InputStream rawEntity = responseHandler.await();
        assertNotNull("Data entity", rawEntity);
        ODataEntity entity = reader.readEntity(rawEntity, ODataFormat.fromContentType(TEST_FORMAT));
        assertEquals("Jose Pavarotti", entity.getProperty("Name").getValue().toString());
        LOG.info("Single entity:  {}", prettyPrint(entity));

        responseHandler.reset();

        olingoApp.uread(edm, TEST_PRODUCT, null, null, responseHandler);
        rawEntity = responseHandler.await();
        entity = reader.readEntity(rawEntity, ODataFormat.fromContentType(TEST_FORMAT));
        assertEquals("20.9", entity.getProperty("Price").getValue().toString());
        LOG.info("Single Entity:  {}", prettyPrint(entity));

        responseHandler.reset();
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put(SystemQueryOptionKind.EXPAND.toString(), CATEGORIES);

        olingoApp.uread(edm, TEST_PRODUCT, queryParams, null, responseHandler);

        rawEntity = responseHandler.await();
        entity = reader.readEntity(rawEntity, ODataFormat.fromContentType(TEST_FORMAT));
        LOG.info("Single People entity with expanded Categories relation:  {}", prettyPrint(entity));
    }*/

    @Test
    public void testReadUpdateProperties() throws Exception {
        // test simple property Airports.Name
    	/*
        final TestOlingo3ResponseHandler<ODataPrimitiveValue> propertyHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.read(edm, TEST_PERSON_SIMPLE_PROPERTY, null, null, propertyHandler);

        ODataPrimitiveValue name = propertyHandler.await();
        assertEquals("Jose Pavarotti", name.toString());
        LOG.info("Person name property value {}", name.asPrimitive());

        final TestOlingo3ResponseHandler<ODataPrimitiveValue> valueHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.read(edm, TEST_PERSON_SIMPLE_PROPERTY_VALUE, null, null, valueHandler);
        ODataPrimitiveValue nameValue = valueHandler.await();
        assertEquals("Jose Pavarotti", name.toString());
        LOG.info("Person name property value {}", nameValue);
        */
    	
    	// Correct request
    	// <d:Name xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">Jose Pavarotti1111111111</d:Name>
    	// Content-Type: application/xml

        TestOlingo3ResponseHandler<HttpStatusCode> statusHandler = new TestOlingo3ResponseHandler<>();
        // All properties updates (simple and complex) are performing through ODataProperty object
        ODataProperty clientProperty = objFactory.newPrimitiveProperty("Name", objFactory.newPrimitiveValueBuilder().buildString("Updated Jose Pavarotti"));
        olingoApp.update(edm, TEST_PERSON + "/Name", null, clientProperty, statusHandler);
        HttpStatusCode statusCode = statusHandler.await();
        assertEquals(HttpStatusCode.NO_CONTENT, statusCode);
        LOG.info("Name property updated with status {}", statusCode.getStatusCode());

        // Check for updated property by reading entire entity again
        final TestOlingo3ResponseHandler<ODataEntity> responseHandler = new TestOlingo3ResponseHandler<>();

        olingoApp.read(edm, TEST_PERSON, null, null, responseHandler);
        ODataEntity entity = responseHandler.await();
        assertEquals("Updated Jose Pavarotti", entity.getProperty("Name").getValue().toString());
        LOG.info("Updated Single Entity:  {}", prettyPrint(entity));
    }

    /*
    @Test
    public void testReadCount() throws Exception {
        final TestOlingo4ResponseHandler<Long> countHandler = new TestOlingo4ResponseHandler<>();

        olingoApp.read(edm, PEOPLE + COUNT_OPTION, null, null, countHandler);
        Long count = countHandler.await();
        assertEquals(20, count.intValue());
        LOG.info("People count: {}", count);
    }

    @Test
    public void testCreateUpdateDeleteEntity() throws Exception {

        // create an entity to update
        final TestOlingo4ResponseHandler<ClientEntity> entryHandler = new TestOlingo4ResponseHandler<>();

        olingoApp.create(edm, PEOPLE, null, createEntity(), entryHandler);

        ClientEntity createdEntity = entryHandler.await();
        LOG.info("Created Entity:  {}", prettyPrint(createdEntity));

        final TestOlingo4ResponseHandler<HttpStatusCode> statusHandler = new TestOlingo4ResponseHandler<>();
        ClientEntity updateEntity = createEntity();
        updateEntity.getProperties().add(objFactory.newPrimitiveProperty("MiddleName", objFactory.newPrimitiveValueBuilder().buildString("Middle")));
        olingoApp.update(edm, TEST_CREATE_PEOPLE, null, updateEntity, statusHandler);
        statusHandler.await();

        statusHandler.reset();
        updateEntity = createEntity();
        updateEntity.getProperties().add(objFactory.newPrimitiveProperty("MiddleName", objFactory.newPrimitiveValueBuilder().buildString("Middle Patched")));
        olingoApp.patch(edm, TEST_CREATE_PEOPLE, null, updateEntity, statusHandler);
        statusHandler.await();

        entryHandler.reset();
        olingoApp.read(edm, TEST_CREATE_PEOPLE, null, null, entryHandler);
        ClientEntity updatedEntity = entryHandler.await();
        LOG.info("Updated Entity successfully:  {}", prettyPrint(updatedEntity));

        statusHandler.reset();
        olingoApp.delete(TEST_CREATE_PEOPLE, null, statusHandler);
        HttpStatusCode statusCode = statusHandler.await();
        LOG.info("Deletion of Entity was successful:  {}: {}", statusCode.getStatusCode(), statusCode.getInfo());

        try {
            LOG.info("Verify Delete Entity");

            entryHandler.reset();
            olingoApp.read(edm, TEST_CREATE_PEOPLE, null, null, entryHandler);

            entryHandler.await();
            fail("Entity not deleted!");
        } catch (Exception e) {
            LOG.info("Deleted entity not found: {}", e.getMessage());
        }
    }

    @Test
    public void testBatchRequest() throws Exception {

        final List<Olingo3BatchRequest> batchParts = new ArrayList<>();

        // 1. Edm query
        batchParts.add(Olingo3BatchQueryRequest.resourcePath(Constants.METADATA).resourceUri(TEST_SERVICE_BASE_URL).build());

        // 2. Query entity set
        batchParts.add(Olingo3BatchQueryRequest.resourcePath(PEOPLE).resourceUri(TEST_SERVICE_BASE_URL).build());

        // 3. Read entity
        batchParts.add(Olingo3BatchQueryRequest.resourcePath(TEST_PEOPLE).resourceUri(TEST_SERVICE_BASE_URL).build());

        // 4. Read with expand
        final HashMap<String, String> queryParams = new HashMap<>();
        queryParams.put(SystemQueryOptionKind.EXPAND.toString(), TRIPS);
        batchParts.add(Olingo3BatchQueryRequest.resourcePath(TEST_PEOPLE).queryParams(queryParams).resourceUri(TEST_SERVICE_BASE_URL).build());

        // 5. Create entity
        final ClientEntity clientEntity = createEntity();
        batchParts.add(Olingo3BatchChangeRequest.resourcePath(PEOPLE).resourceUri(TEST_SERVICE_BASE_URL).contentId(TEST_CREATE_RESOURCE_CONTENT_ID).operation(Operation.CREATE)
            .body(clientEntity).build());

        // 6. Update entity
        clientEntity.getProperties().add(objFactory.newPrimitiveProperty("MiddleName", objFactory.newPrimitiveValueBuilder().buildString("Lewis")));
        batchParts.add(Olingo3BatchChangeRequest.resourcePath(TEST_CREATE_PEOPLE).resourceUri(TEST_SERVICE_BASE_URL).contentId(TEST_UPDATE_RESOURCE_CONTENT_ID)
            .operation(Operation.UPDATE).body(clientEntity).build());

        // 7. Delete entity
        batchParts.add(Olingo3BatchChangeRequest.resourcePath(TEST_CREATE_PEOPLE).resourceUri(TEST_SERVICE_BASE_URL).operation(Operation.DELETE).build());

        // 8. Read to verify entity delete
        batchParts.add(Olingo3BatchQueryRequest.resourcePath(TEST_CREATE_PEOPLE).resourceUri(TEST_SERVICE_BASE_URL).build());

        final TestOlingo4ResponseHandler<List<Olingo3BatchResponse>> responseHandler = new TestOlingo4ResponseHandler<>();
        olingoApp.batch(edm, null, batchParts, responseHandler);

        final List<Olingo3BatchResponse> responseParts = responseHandler.await(15, TimeUnit.MINUTES);
        assertEquals("Batch responses expected", 8, responseParts.size());

        assertNotNull(responseParts.get(0).getBody());

        final ClientEntitySet clientEntitySet = (ClientEntitySet)responseParts.get(1).getBody();
        assertNotNull(clientEntitySet);
        LOG.info("Batch entity set:  {}", prettyPrint(clientEntitySet));

        ClientEntity returnClientEntity = (ClientEntity)responseParts.get(2).getBody();
        assertNotNull(returnClientEntity);
        LOG.info("Batch read entity:  {}", prettyPrint(returnClientEntity));

        returnClientEntity = (ClientEntity)responseParts.get(3).getBody();
        assertNotNull(returnClientEntity);
        LOG.info("Batch read entity with expand:  {}", prettyPrint(returnClientEntity));

        ClientEntity createdClientEntity = (ClientEntity)responseParts.get(4).getBody();
        assertNotNull(createdClientEntity);
        assertEquals(TEST_CREATE_RESOURCE_CONTENT_ID, responseParts.get(4).getContentId());
        LOG.info("Batch created entity:  {}", prettyPrint(returnClientEntity));

        assertEquals(HttpStatusCode.NO_CONTENT.getStatusCode(), responseParts.get(5).getStatusCode());
        assertEquals(TEST_UPDATE_RESOURCE_CONTENT_ID, responseParts.get(5).getContentId());
        assertEquals(HttpStatusCode.NO_CONTENT.getStatusCode(), responseParts.get(6).getStatusCode());
        assertEquals(HttpStatusCode.NOT_FOUND.getStatusCode(), responseParts.get(7).getStatusCode());
    }

    private ODataEntity createEntity() {
    	ODataEntity clientEntity = objFactory.newEntity(null);

        clientEntity.getProperties().add(objFactory.newPrimitiveProperty("UserName", objFactory.newPrimitiveValueBuilder().buildString("lewisblack")));
        clientEntity.getProperties().add(objFactory.newPrimitiveProperty("FirstName", objFactory.newPrimitiveValueBuilder().buildString("Lewis")));
        clientEntity.getProperties().add(objFactory.newPrimitiveProperty("LastName", objFactory.newPrimitiveValueBuilder().buildString("Black")));

        return clientEntity;
    }
    */

    private static String prettyPrint(ODataEntitySet entitySet) {
        StringBuilder builder = new StringBuilder();
        builder.append("[\n");
        for (ODataEntity entity : entitySet.getEntities()) {
            builder.append(prettyPrint(entity.getProperties(), 1)).append('\n');
        }
        builder.append("]\n");
        return builder.toString();
    }

    private static String prettyPrint(ODataEntity entity) {
        return prettyPrint(entity.getProperties(), 0);
    }

    @SuppressWarnings("unchecked")
    private static String prettyPrint(Map<String, Object> properties, int level) {
        StringBuilder b = new StringBuilder();
        Set<Entry<String, Object>> entries = properties.entrySet();

        for (Entry<String, Object> entry : entries) {
            intend(b, level);
            b.append(entry.getKey()).append(": ");
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = prettyPrint((Map<String, Object>)value, level + 1);
            } else if (value instanceof Calendar) {
                Calendar cal = (Calendar)value;
                value = DateFormat.getInstance().format(cal.getTime());
            }
            b.append(value).append("\n");
        }
        // remove last line break
        b.deleteCharAt(b.length() - 1);
        return b.toString();
    }

    private static String prettyPrint(Collection<ODataProperty> properties, int level) {
        StringBuilder b = new StringBuilder();

        for (Object property : properties) {
            intend(b, level);
            if (property instanceof ODataProperty) {
            	ODataProperty entry = (ODataProperty)property;
            	ODataValue value = entry.getValue();
                if (value.isCollection()) {
                    ODataCollectionValue<ODataValue> cclvalue = value.asCollection();
                    b.append(entry.getName()).append(": ");
                    // TODO : Fix print algo there
                    // b.append(prettyPrint((cclvalue.asJavaCollection(), level + 1));
                } else if (value.isComplex()) {
                    ODataComplexValue<CommonODataProperty> cpxvalue = value.asComplex();
                    b.append(prettyPrint(cpxvalue.asJavaMap(), level + 1));
                } else if (value.isPrimitive()) {
                    b.append(entry.getName()).append(": ");
                    b.append(entry.getValue()).append("\n");
                }
            } else {
                b.append(property.toString()).append("\n");
            }

        }
        return b.toString();
    }

    private static void intend(StringBuilder builder, int intendLevel) {
        for (int i = 0; i < intendLevel; i++) {
            builder.append("  ");
        }
    }
    
    private static final class TestOlingo3ResponseHandler<T> implements Olingo3ResponseHandler<T> {
        private T response;
        private Exception error;
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(T response, Map<String, String> responseHeaders) {
            this.response = response;
            if (LOG.isDebugEnabled()) {
                if (response instanceof ODataEntitySet) {
                    //LOG.debug("Received response: {}", prettyPrint((ODataEntitySet)response));
                } else if (response instanceof ODataEntity) {
                    //LOG.debug("Received response: {}", prettyPrint((ODataEntity)response));
                } else {
                    LOG.debug("Received response: {}", response);
                }
            }
            latch.countDown();
        }

        @Override
        public void onException(Exception ex) {
            error = ex;
            latch.countDown();
        }

        @Override
        public void onCanceled() {
            error = new IllegalStateException("Request Canceled");
            latch.countDown();
        }

        public T await() throws Exception {
            return await(TIMEOUT, TimeUnit.SECONDS);
        }

        public T await(long timeout, TimeUnit unit) throws Exception {
            assertTrue("Timeout waiting for response", latch.await(timeout, unit));
            if (error != null) {
                throw error;
            }
            assertNotNull("Response", response);
            return response;
        }

        public void reset() {
            latch.countDown();
            latch = new CountDownLatch(1);
            response = null;
            error = null;
        }
    }
}
