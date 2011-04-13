package com.bigdata.rdf.sail.webapp;

/**
 * Interface declaring the <code>config-param</code>s understood by the
 * {@link BigdataRDFServletContextListener}.
 */
public interface ConfigParams {

    /**
     * The property file (for a standalone bigdata instance) or the jini
     * configuration file (for a bigdata federation). The file must end with
     * either ".properties" or ".config".
     */
    String PROPERTY_FILE = "property-file";

    /**
     * The default bigdata namespace of for the triple or quad store
     * instance to be exposed (there can be many triple or quad store
     * instances within a bigdata instance).
     */
    String NAMESPACE = "namespace";

    /**
     * The size of the thread pool used to service SPARQL queries -OR- ZERO
     * (0) for an unbounded thread pool (default
     * {@value #DEFAULT_QUERY_THREAD_POOL_SIZE}).
     */
    String QUERY_THREAD_POOL_SIZE = "query-thread-pool-size";
    
    int DEFAULT_QUERY_THREAD_POOL_SIZE = 16;
    
    /**
     * Force a compacting merge of all shards on all data services in a
     * bigdata federation (optional, default <code>false</code>).
     * 
     * <strong>This option should only be used for benchmarking
     * purposes.</strong>
     */
    String FORCE_OVERFLOW = "force-overflow";

    /**
     * The commit time against which the server will assert a read lock by
     * holding open a read-only transaction against that commit point
     * (optional). When given, queries will default to read against this
     * commit point. Otherwise queries will default to read against the most
     * recent commit point on the database. Regardless, each query will be
     * issued against a read-only transaction.
     */
    String READ_LOCK = "read-lock";
    
}
