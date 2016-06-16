package com.aol.store;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by rushabhshroff on 1/9/15.
 */
public class PigSolrDataStore extends StoreFunc {

    private static final String DELIM = ",";
    private static final String EMPTY = "";

    private RecordWriter<String[], Tuple> writer = null;
    private String collection = EMPTY;
    private String idField = EMPTY;
    private Boolean cloud = false;
    private Boolean partialUpdate = false;
    private String ttlParamName = EMPTY;
    private String ttlExpression = EMPTY;
    private String solrVersion = EMPTY;

    private String[] schema;
    private static final String TRUE = "true";

    public PigSolrDataStore(String[] args) {
        // TODO: better way to set this?
        System.setProperty("zookeeper.sasl.client", "false");

        for (String arg : args) {
            String[] argSplit = arg.split("=");

            if (argSplit.length == 2) {
                if (arg.startsWith("solrVersion=")) {
                    this.solrVersion = argSplit[1];
                } else if (arg.startsWith("collection=")) {
                    this.collection = argSplit[1];
                } else if (arg.startsWith("idField=")) {
                    this.idField = argSplit[1];
                } else if (arg.startsWith("cloud=")) {
                    this.cloud = isTrueString(argSplit[1]);
                } else if (arg.startsWith("partialUpdate=")) {
                    this.partialUpdate = isTrueString(argSplit[1]);
                } else if (arg.startsWith("ttlParamName=")) {
                    this.ttlParamName = argSplit[1];
                } else if (arg.startsWith("ttlExpression=")) {
                    this.ttlExpression = argSplit[1];
                }
            }
        }

        if (args.length > 7) {
            // schema means we expected "a"="b"
            //this.schema = args[2].split(DELIM);
            this.schema = this.idField.split(DELIM);
        } else {
            // null schema means we expected "a=b"
            this.schema = null;
        }
    }


    protected static boolean isTrueString(String value) {
        return TRUE.equalsIgnoreCase(value);
    }


    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new PigSolrOutputFormat(new PigSolrStoreWriter());
    }

    @Override
    public void setStoreLocation(String location, Job job) {
        // as per solr documentation this list can be a csv of zookeepers

        DataStoreType dataStoreType;

        if (this.solrVersion.startsWith("SOLR4")) {
            dataStoreType = DataStoreType.SOLR4;
        } else {
            if (this.cloud) {
                location = location.replaceAll("zookeeper://", "");
            }
            dataStoreType = DataStoreType.SOLR5;
        }

        job.getConfiguration().set("solr.version", dataStoreType.getName());
        job.getConfiguration().set("solr.url", location);
        job.getConfiguration().set("solr.collection", this.collection);
        job.getConfiguration().set("solr.field.id", this.idField);
        job.getConfiguration().setBoolean("solr.cloud.enabled", this.cloud);
        job.getConfiguration().setBoolean("solr.update.partial", this.partialUpdate);
        job.getConfiguration().set("solr.ttl.expression", this.ttlExpression);
        job.getConfiguration().set("solr.ttl.paramName", this.ttlParamName);
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        //noinspection unchecked
        this.writer = writer;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        try {
            this.writer.write(this.schema, tuple);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
