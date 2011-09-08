package com.bigdata.rdf.sail.webapp;

/**
 * Configuration object.
 */
public class SparqlEndpointConfig {

    /**
     * The default namespace.
     */
    final public String namespace;

    /**
     * The default timestamp used to query the default namespace. The server
     * will obtain a read only transaction which reads from the commit point
     * associated with this timestamp.
     */
    final public long timestamp;

    /**
     * The #of threads to use to handle SPARQL queries -or- ZERO (0) for an
     * unbounded pool.
     */
    final public int queryThreadPoolSize;
    
    /**
     * @see com.bigdata.rdf.sail.BigdataSail.Options#NATIVE_SPARQL
     */
    final public boolean nativeSparql;

    public SparqlEndpointConfig(final String namespace, final long timestamp,
            final int queryThreadPoolSize, final boolean nativeSparql) {

        if (namespace == null)
            throw new IllegalArgumentException();

        this.namespace = namespace;

        this.timestamp = timestamp;

        this.queryThreadPoolSize = queryThreadPoolSize;
        
        this.nativeSparql = nativeSparql;

    }

}
