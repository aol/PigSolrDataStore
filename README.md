**Please Note:** This repository contains known security vulnerabilities. Use at your own risk!

Pig Solr Data Store
==============

The PigSolrDataStore is a library to directly index solr from pig using a single STORE function. It is a configurable library which works with both Solr4 and Solr5. Check the examples below for knowing how to use the PigSolrDataStore.

The PigSolrDataStore can be configured using the following parameters:

* solrVersion (Required) - version of the solr being used. It can be either "SOLR4" or "SOLR5". 
* collection (Required) - the collection we are updating.
* idField (Required) - the unique key of a document if it exists.
* cloud (Optional) - whether or not we are storing documents in a cloud server or not. (Not needed for SOLR4)
* partialUpdate (Optional) - whether or not we are doing partial updates or complete doc replacements. (requires unique key, not needed for SOLR4)
* ttlParamName (Optional) - ```solr.processor.DocExpirationUpdateProcessorFactory``` property, example ```_ttl_```, to use that can be coupled with expirationFieldName, example ```_expire_at_```, to set a document expiration datetime. (Not needed for SOLR4)
* ttlExpression (Optional) - a date math expression, defaults to "+10080MINUTES", which is an offset from the request date/time. (Not needed for SOLR4)

These parameters can be supplied to the PigSolrStore in any order.

Run ```mvn clean install``` and inside the target directory find the PigSolrDataStore-x.x.x-jar-with-dependencies.jar. 



Solr 4
------

Can be index using the following STORE function
```
STORE adlogs INTO 'http://solr.aol.com:8983/solr' USING com.aol.store.PigSolrDataStore('solrVersion=SOLR4', 'collection=collection1', 'idField=someId')
```

Solr 5
------

* a)

In the case where we are submitting to a solr server we go direct to it.  The url would be something like:
```
http://solr.aol.com:8983/solr/collection1
```

and the STORE function looks like
```
STORE adlogs INTO 'http://solr.aol.com:8983/solr/collection1' USING com.aol.store.PigSolrDataStore('solrVersion=SOLR5', 'collection=collection1', 'idField=someId')
```

* b)

In the case where we are submitting to a solr cloud server we access zookeeper.  The url would be something like:
```
zookeeper://zk.aol.com:2181
```

This can also be a csv to access an ensemble:
```
zookeeper://zk1.aol.com:2181,zk2.aol.com:2181,zk3.aol.com:2181
```

and the STORE function looks like
```
STORE adlogs INTO 'zookeeper://zk.aol.com:2181' USING com.aol.store.PigSolrDataStore('solrVersion=SOLR5',
                                'collection=collection1',
                                'idField=someId',
                                'cloud=true');
```

Partial Update
--------------

If set to true a unique key is required, both in the settings here and in your solr schema.  This means that a new
document will be created if the unique key does not exist and if it does then only the provided fields will be updated.
