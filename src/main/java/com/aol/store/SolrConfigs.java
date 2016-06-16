package com.aol.store;

import org.apache.commons.lang.StringUtils;

/**
 * Created by rushabhshroff on 6/6/16.
 */
public class SolrConfigs {

    private DataStoreType solrVersion;

    // the collection we will be updating
    private String collection;
    // how many times we will retry if the connection fails
    private int retries = 0;
    // the unique id of the document
    private String idField;
    // whether or not we want to override documents or partial update
    private Boolean partialUpdate = false;

    private String ttlParamName = "";
    private String ttlExpression = "";
    private Boolean ttlEnabled = false;
    private String url;
    private Boolean cloud = false;
    private final String TTL_DEFAULT_EXPRESSION = "+10080MINUTES";
    private int bufferSize = 10;


    public SolrConfigs(DataStoreType solrVersion, String url, boolean cloud, boolean partialUpdate, String collection,
                       String idField, int retries, String ttlParamName, String ttlExpression, int bufferSize) {
        this.solrVersion = solrVersion;
        this.url = url;
        this.cloud = cloud;
        this.retries = retries;
        this.collection = collection;
        this.partialUpdate = partialUpdate;
        this.idField = idField;
        this.bufferSize = bufferSize;

        if (StringUtils.isNotBlank(ttlParamName)) {

            this.ttlEnabled = true;
            this.ttlParamName = ttlParamName;
            this.ttlExpression = TTL_DEFAULT_EXPRESSION;

            if (StringUtils.isNotBlank(ttlExpression))
                this.ttlExpression = ttlExpression;
        }
    }

    public String getUrl() {
        return url;
    }


    public DataStoreType getSolrVersion() {
        return solrVersion;
    }

    public String getCollection() {
        return collection;
    }

    public int getRetries() {
        return retries;
    }

    public String getIdField() {
        return idField;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public String getTtlParamName() {
        return ttlParamName;
    }

    public String getTtlExpression() {
        return ttlExpression;
    }

    public boolean isTtlEnabled() {
        return ttlEnabled;
    }

    public boolean isCloud() {
        return cloud;
    }

    public int getBufferSize() {
        return bufferSize;
    }

}
