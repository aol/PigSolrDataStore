package com.aol.store;

import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PigSolrStoreWriter {

    private static final String EQUALS = "=";
    private static final String COLLECTION = "collection";

    // This is the solr server we want to connect to
    private SolrClient solrServer;
    private SolrConfigs solrConfigs;

    private Collection<Tuple> documentsBuffer;

    public void open(SolrConfigs solrConfigs) throws MalformedURLException {

        this.solrConfigs = solrConfigs;
        switch (solrConfigs.getSolrVersion()) {
            case SOLR4:
                this.solrServer = createHttpSolrServer(solrConfigs.getUrl());
                break;
            default:
                this.solrServer = solrConfigs.isCloud() ?
                        createSolrServer(solrConfigs.getUrl(), solrConfigs.getIdField()) :
                        createSolrServer(solrConfigs.getUrl());
        }

        if (solrConfigs.getBufferSize() > 0)
            this.documentsBuffer = new ArrayList(solrConfigs.getBufferSize());
        else {
            this.documentsBuffer = new ArrayList();
        }
    }

    @SuppressWarnings("deprecation")
    private SolrClient createSolrServer(String url, String idField) {
        SystemDefaultHttpClient httpClient = new SystemDefaultHttpClient();
        CloudSolrClient server = new CloudSolrClient(url, httpClient);
        server.setIdField(idField);
        server.setParallelUpdates(true);
        return server;
    }

    @SuppressWarnings("deprecation")
    private SolrClient createSolrServer(String url) {
        SystemDefaultHttpClient httpClient = new SystemDefaultHttpClient();
        return new HttpSolrClient(url, httpClient);
    }

    @SuppressWarnings("deprecation")
    private SolrClient createHttpSolrServer(String url) {

        if (!url.endsWith("/"))
            url = url + "/";

        SystemDefaultHttpClient httpClient = new SystemDefaultHttpClient();
        return new HttpSolrServer(url, httpClient);
    }

    public UpdateRequest getUpdateRequest(String[] schema) throws IOException {
        UpdateRequest add = new UpdateRequest();

        for(Tuple tuple : documentsBuffer) {
            add.add(schema == null ? convertToSolr(tuple) : convertToSolr(schema, tuple));
            add.setParam(COLLECTION, solrConfigs.getCollection());
            if (solrConfigs.isTtlEnabled()) {
                add.setParam(solrConfigs.getTtlParamName(), solrConfigs.getTtlExpression());
            }
        }
        return add;
    }


    public void write(String[] schema, Tuple tuple) throws IOException {
        documentsBuffer.add(tuple);
        if (this.documentsBuffer.size() >= solrConfigs.getBufferSize()) {
            UpdateRequest updateRequest = getUpdateRequest(schema);
            process(updateRequest);
        }
    }

    private void process(UpdateRequest add) throws IOException {
        for (int attempt = 0; attempt < solrConfigs.getRetries(); attempt++) {
            // TODO: ? com.aol.solr.SolrWriter.process(SolrWriter.java:49) ... 20 more Caused by:
            // TODO: ?  java.net.SocketException: Connection timed out at java.net.SocketInputStream.socketRead0(Native Method) at
            // TODO: ? com.aol.solr.SolrWriter.process(SolrWriter.java:49) ... 20 more Caused by:
            // TODO: ?  org.apache.zookeeper.KeeperException$SessionExpiredException: KeeperErrorCode = Session expired for /collections/dex/state.json at
            try {
                send(add);
                this.documentsBuffer.clear();
            } catch (SolrServerException e) {
                // we swallow this exception due to reties
            } catch (SolrException e) {
                // we swallow this exception due to reties
            }
        }
        // final attempt; if it fails we throw an exception
        try {
            send(add);
            this.documentsBuffer.clear();
        } catch (SolrServerException e) {
            this.documentsBuffer.clear();
            throw new IOException(String.format("with %d retries", solrConfigs.getRetries()), e);
        } catch (SolrException e) {
            this.documentsBuffer.clear();
            throw new IOException(String.format("with %d retries", solrConfigs.getRetries()), e);
        }
    }

    public UpdateResponse send(UpdateRequest add) throws IOException, SolrServerException {
        switch (solrConfigs.getSolrVersion()) {
            case SOLR4:
                UpdateResponse response = new UpdateResponse();
                response.setResponse(this.solrServer.request(add));
                return response;
            default:
                return add.process(this.solrServer);
        }
    }

    public SolrInputDocument convertToSolr(String[] schema, Tuple tuple) throws ExecException {
        SolrInputDocument document = new SolrInputDocument();
        for (int i = 0; i < tuple.size(); i++) {
            addField(document, schema[i], tuple.get(i));
        }
        return document;
    }

    public SolrInputDocument convertToSolr(Tuple tuple) throws IOException {
        SolrInputDocument document = new SolrInputDocument();
        convertToSolr(tuple, document);
        return document;
    }

    public void convertToSolr(Tuple tuple, SolrInputDocument document) throws IOException {
        if (tuple != null) {
            for (Object entry : tuple.getAll()) {
                if (entry != null) {
                    if (entry instanceof String) {
                        String[] values = ((String) entry).split(EQUALS, 2);
                        if (values.length == 2) {
                            addField(document, values[0], values[1]);
                        }
                    } else if (entry instanceof Tuple) {
                        convertToSolr((Tuple) entry, document);
                    } else {
                        throw new IOException("Unknown type: " + entry.getClass().getName());
                    }
                }
            }

        }
    }

    private void addField(SolrInputDocument document, String name, Object value) {
        if (solrConfigs.getIdField().equals(name)) {
            document.addField(name, value);
        } else if (solrConfigs.isPartialUpdate()) {
            // if this is not the key then we do partial updates
            Map<String, Object> fieldModifier = new HashMap<String, Object>(1);
            fieldModifier.put("set", value);
            document.addField(name, fieldModifier);
        } else {
            document.addField(name, value);
        }
    }

    public void storeRemaining(String [] schema) throws IOException {
        if (this.documentsBuffer.size() > 0) {
            UpdateRequest updateRequest = getUpdateRequest(schema);
            process(updateRequest);
        }

    }

    public void close() throws IOException {
        this.solrServer.close();
    }

    public void commit() {
        try {
            this.solrServer.commit(solrConfigs.getCollection(), false, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
