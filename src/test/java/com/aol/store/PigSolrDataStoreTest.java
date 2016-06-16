package com.aol.store;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by rushabhshroff on 6/8/16.
 */
public class PigSolrDataStoreTest extends SolrTestCaseJ4 {

    private Collection setupBuffer(PigSolrStoreWriter writer, SolrConfigs solrConfigs) throws Exception {
        Field field = writer.getClass().getDeclaredField("documentsBuffer");
        field.setAccessible(true);
        Collection documentsBuffer = new ArrayList(solrConfigs.getBufferSize());
        field.set(writer, documentsBuffer);
        return documentsBuffer;
    }

    private void setupBuffer(PigSolrStoreWriter writer, SolrConfigs solrConfigs, Collection tuples) throws Exception {
        Collection documentsBuffer = setupBuffer(writer, solrConfigs);
        documentsBuffer.addAll(tuples);
    }

    private void setupMockSolrServer(PigSolrStoreWriter writer, SolrClient solrClient) throws Exception {
        Field field = writer.getClass().getDeclaredField("solrServer");
        field.setAccessible(true);
        field.set(writer, solrClient);
    }

    private SolrConfigs getConfigs (DataStoreType dataStoreType) {
        return new SolrConfigs(dataStoreType,
                "http://localhost:8983/solr",
                true,
                false,
                "example",
                "id",
                2,
                "",
                "",
                1);
    }

    private void setupMockConfigs(PigSolrStoreWriter writer, SolrConfigs solrConfigs) throws Exception {
        Field field = writer.getClass().getDeclaredField("solrConfigs");
        field.setAccessible(true);
        field.set(writer, solrConfigs);
    }

    private PigSolrStoreWriter setupMockSolrWriter(DataStoreType dataStoreType) throws Exception {
        return setupMockSolrWriter(dataStoreType, false);
    }

    private PigSolrStoreWriter setupMockSolrWriter(DataStoreType dataStoreType, Boolean isCloud) throws Exception {
        PigSolrStoreWriter writer = new PigSolrStoreWriter();
        SolrConfigs solrConfigs = getConfigs(dataStoreType);

        setupMockConfigs(writer, solrConfigs);

        Collection tuples = new ArrayList();
        tuples.add(getTuple());

        setupBuffer(writer, solrConfigs, tuples);

        return writer;
    }

    private Tuple getTuple() {
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple newTuple = tupleFactory.newTuple();
        newTuple.append("id=12345");
        newTuple.append("title_s=some_title");

        return newTuple;
    }


    private NamedList<Object> getResponseObject() {
        NamedList<Object> responseObjs = new NamedList<Object>();
        responseObjs.add("something1", 1);
        responseObjs.add("something2", 2);

        return responseObjs;
    }

    private void assertResponseObject(NamedList<Object> writerResponse) {
        int count = 0;
        for (Object resp : writerResponse) {
            if (resp.toString().equals("something1=1")) {
                count++;
            }
            if (resp.toString().equals("something2=2")) {
                count++;
            }
        }

        Assert.assertEquals(count, 2);
    }

    private SolrClient getSolrClient() {
        return new EmbeddedSolrServer(h.getCoreContainer(), h.getCore().getName());
    }

    private SolrClient getMockSolrClient() {
        return Mockito.mock(SolrClient.class);
    }

    @Test
    public void testConvertToSolr() throws Exception {
        PigSolrStoreWriter writer = setupMockSolrWriter(DataStoreType.SOLR4);

        SolrInputDocument documents = writer.convertToSolr(getTuple());

        Assert.assertTrue(documents.size() == 2);
        Assert.assertEquals(documents.getFieldValue("id"), "12345");
        Assert.assertEquals(documents.getFieldValue("title_s"), "some_title");
    }

    @Test
    public void testGetUpdateRequest() throws Exception {
        PigSolrStoreWriter writer = setupMockSolrWriter(DataStoreType.SOLR4);

        UpdateRequest updateRequest = writer.getUpdateRequest(null);

        List<SolrInputDocument> documents = updateRequest.getDocuments();

        Assert.assertTrue(documents.size() == 1);
        Assert.assertEquals(documents.get(0).getFieldValue("id"), "12345");
        Assert.assertEquals(documents.get(0).getFieldValue("title_s"), "some_title");
    }

    @Test
    public void testSolr4() throws Exception {
        PigSolrStoreWriter writer = setupMockSolrWriter(DataStoreType.SOLR4);

        SolrClient solrClient = getMockSolrClient();
        setupMockSolrServer(writer, solrClient);

        UpdateRequest request = writer.getUpdateRequest(null);
        Mockito.when(solrClient.request(request)).thenReturn(getResponseObject());

        UpdateResponse response = writer.send(request);

        NamedList<Object> writerResponse = response.getResponse();
        Assert.assertTrue(writerResponse.size() == 2);
        assertResponseObject(writerResponse);

        Mockito.verify(solrClient).request(request);
    }

    @Test
    public void testSolrIT() throws Exception {
        setupSolrCore();

        PigSolrStoreWriter writer = setupMockSolrWriter(DataStoreType.SOLR4);

        SolrClient solrClient = getSolrClient();
        setupMockSolrServer(writer, solrClient);

        writer.write(null, getTuple());

        SolrDocument document = solrClient.getById("12345");

        Assert.assertTrue(document.size() == 3); // The id, title_s and the _version_
        Assert.assertTrue(document.get("title_s").equals("some_title"));
    }

    private void setupSolrCore() throws Exception {
        System.setProperty("solr.allow.unsafe.resourceloading", "true");

        SolrTestCaseJ4.initCore(getSolrConfig(),
                getSchema(),
                getSolrHome());
    }

    private String getSolrHome() {
        return System.getProperty("user.dir") + "/solr";
    }

    private String getSchema() {
        return System.getProperty("user.dir") + "/solr/conf/managed_schema";
    }

    private String getSolrConfig() {
        return System.getProperty("user.dir") + "/solr/conf/solrconfig.xml";
    }
}
