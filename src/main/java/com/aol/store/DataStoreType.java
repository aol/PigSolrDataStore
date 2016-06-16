package com.aol.store;

/**
 * Created by rushabhshroff on 6/5/16.
 */
public enum DataStoreType {
    SOLR4("v4"),
    SOLR5("v5");

    String name;
    DataStoreType(String version) {
        this.name = version;
    }

    public String getName() {
        return name;
    }
}
