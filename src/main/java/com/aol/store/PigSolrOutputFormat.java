package com.aol.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by rushabhshroff on 1/11/15.
 */
public class PigSolrOutputFormat extends OutputFormat {

    private final PigSolrStoreWriter solrWriter;

    public PigSolrOutputFormat(PigSolrStoreWriter solrWriter) {
        this.solrWriter = solrWriter;
    }

    // The schema is needed for any remaining documents in the buffer, check the close method
    private String[] schema = null;

    private SolrConfigs parseSolrConfigs(Configuration config) {
        return new SolrConfigs(
                config.get("solr.version").equals(DataStoreType.SOLR4.getName()) ? DataStoreType.SOLR4 : DataStoreType.SOLR5,
                config.get("solr.url"),
                config.getBoolean("solr.cloud.enabled", false),
                config.getBoolean("solr.update.partial", false),
                config.get("solr.collection"),
                config.get("solr.field.id"),
                2,
                config.get("solr.ttl.paramName"),
                config.get("solr.ttl.expression"),
                10);
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        this.solrWriter.open(parseSolrConfigs(config));
        return new RecordWriter<String[], Tuple>() {
            @Override
            public void write(String[] schema, Tuple tuple) throws IOException, InterruptedException {
                // If the Schema is not set then set it
                if(PigSolrOutputFormat.this.schema == null) {
                    PigSolrOutputFormat.this.schema = schema;
                }

                PigSolrOutputFormat.this.solrWriter.write(schema, tuple);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                PigSolrOutputFormat.this.solrWriter.storeRemaining(PigSolrOutputFormat.this.schema);
                PigSolrOutputFormat.this.solrWriter.commit();
                PigSolrOutputFormat.this.solrWriter.close();
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext context) throws IOException {
            }

            @Override
            public void setupTask(TaskAttemptContext context) throws IOException {
            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext context) throws IOException {
            }

            @Override
            public void abortTask(TaskAttemptContext context) throws IOException {
            }
        };
    }
}
