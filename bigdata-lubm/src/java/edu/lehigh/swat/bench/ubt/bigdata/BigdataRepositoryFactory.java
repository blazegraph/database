/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Apr 17, 2007
 */

package edu.lehigh.swat.bench.ubt.bigdata;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexMetadata.Options;
import com.bigdata.config.Configuration;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.axioms.RdfsAxioms;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;

import edu.lehigh.swat.bench.ubt.api.Repository;
import edu.lehigh.swat.bench.ubt.api.RepositoryFactory;

/**
 * Configuration is hard coded in {@link #create()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo option to force compaction of the journal or data services?
 * 
 * @todo add an option to verify the actual answers that are being produced, not
 *       just the #of query results.
 */
abstract public class BigdataRepositoryFactory extends RepositoryFactory {

    protected static final Logger log = Logger.getLogger(BigdataRepositoryFactory.class);
    
    final String database;
    
    /**
     * The namespace for the tripleStore resource. The value is taken from the
     * value of the {@link Options#NAMESPACE} property using
     * {@link System#getProperty(String, String)} and defaults to
     * {@link Options#DEFAULT_NAMESPACE} if not specified.
     * 
     * @see Options#NAMESPACE
     */
    public final static String getNamespace() {

        return System.getProperty(Options.NAMESPACE, Options.DEFAULT_NAMESPACE);

    }
    
    /**
     * Extended ctor variant passes along the <code>database</code> property
     * from the lubm kb config file.
     * 
     * @param database
     */
    public BigdataRepositoryFactory(String database) {
    
        this.database = database;
        
    }

    /**
     * Return the {@link Properties} that will be used to configure a new
     * {@link AbstractTripleStore} instance. The {@link AbstractTripleStore}
     * will remember the properties with which it was created and use those
     * values each time it is re-opened. The properties are stored in the global
     * row store for the backing {@link IIndexManager}.
     */
    protected Properties getProperties() {

        // pick up system properties as defaults.
        final Properties properties = new Properties(System.getProperties());
        // do not pick up system properties as defaults.
//        final Properties properties = new Properties();
        
        /* 
         * memory only mode.
         */
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        /*
         * For the LTS, this is the name of the backing file created and used by
         * the Journal.
         */
        properties.setProperty(Options.FILE, database);
        
        /*
         * The name of the data directory (LDS).
         */
        properties.setProperty(StoreManager.Options.DATA_DIR, database);
        
        /*
         * The name of the directory in which all of the embedded data services
         * will place their state (EDS).
         */
        properties.setProperty(EmbeddedClient.Options.DATA_DIR, database);
        
        /*
         * disk only (scale out to larger stores without causing the journal to
         * overflow).
         * 
         * You MUST specify Options#FILE for this buffer mode (when using a
         * journal that does not overflow onto index segments).
         */
        properties.setProperty(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskRW.toString());

        /*
         * Override the initial and maximum extent so that they are more suited
         * to large data sets.
         */
        properties.setProperty(Options.INITIAL_EXTENT,""+200*Bytes.megabyte);
        properties.setProperty(Options.MAXIMUM_EXTENT,""+200*Bytes.megabyte);
        
        /*
         * When "true", the store will perform incremental closure as the data
         * are loaded. When "false", the closure will be computed after all data
         * are loaded. (Actually, since we are not loading through the SAIL
         * making this true does not cause incremental TM but it does disable
         * closure, so "false" is what you need here).
         */
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        /*
         * Enable rewrites of high-level queries into native rules (native JOIN
         * execution). (Can be changed without re-loading the data to compare
         * the performance of the Sesame query evaluation against using the
         * native rules to perform query evaluation.)
         */
        properties.setProperty(BigdataSail.Options.NATIVE_JOINS, "true");

        /*
         * Option to restrict ourselves to RDFS only inference. This condition
         * may be compared readily to many other stores.
         * 
         * Note: While we can turn on some kinds of owl processing (e.g.,
         * TransitiveProperty, see below), we can not compute all the necessary
         * entailments (only queries 11 and 13 benefit).
         * 
         * Note: There are no owl:sameAs assertions in LUBM.
         * 
         * Note: lubm query does not benefit from owl:inverseOf.
         * 
         * Note: lubm query does benefit from owl:TransitiveProperty (queries 11
         * and 13).
         * 
         * Note: owl:Restriction (which we can not compute) plus
         * owl:TransitiveProperty is required to get all the answers for LUBM.
         * 
         * @todo disable the backchainer for LDS, EDS, JDS.
         */
        properties.setProperty(Options.AXIOMS_CLASS,RdfsAxioms.class.getName());
//        properties.setProperty(Options.AXIOMS_CLASS,NoAxioms.class.getName());
        
        /*
         * Produce a full closure (all entailments) so that the backward chainer
         * is always a NOP. Note that the configuration properties are stored in
         * the database (in the global row store) so you always get exactly the
         * same configuration that you created when reopening a triple store.
         */
//        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "true");
//        properties.setProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES, "true");
        
        /*
         * Additional owl inferences. LUBM only both inverseOf and
         * TransitiveProperty of those that we support (owl:sameAs,
         * owl:inverseOf, owl:TransitiveProperty), but not owl:sameAs.
         */
        properties.setProperty(Options.FORWARD_CHAIN_OWL_INVERSE_OF, "true");
        properties.setProperty(Options.FORWARD_CHAIN_OWL_TRANSITIVE_PROPERTY, "true");

        // Note: FastClosure is the default.
//        properties.setProperty(Options.CLOSURE_CLASS, FullClosure.class.getName());

        /*
         * Various things that effect native rule execution.
         */
//        properties.setProperty(Options.FORCE_SERIAL_EXECUTION, "false");
        
//      properties.setProperty(Options.BUFFER_CAPACITY, "20000");
        properties.setProperty(Options.CHUNK_CAPACITY, "100");
        properties.setProperty(Options.CHUNK_OF_CHUNKS_CAPACITY, "64"); // should be near branching factor, why not just use branching factor?

//      properties.setProperty(Options.QUERY_BUFFER_CAPACITY, "10000");
//      properties.setProperty(Options.FULLY_BUFFERED_READ_THRESHOLD, "10000");

        /*
         * Turn off incremental closure in the DataLoader object.
         */
        properties.setProperty(
                com.bigdata.rdf.store.DataLoader.Options.CLOSURE,
                DataLoader.ClosureEnum.None.toString());

        /*
         * Turn off commit in the DataLoader object. We do not need to commit
         * anything until we have loaded all the data and computed the closure
         * over the database.
         */
        properties.setProperty(
                com.bigdata.rdf.store.DataLoader.Options.COMMIT,
                DataLoader.CommitEnum.None.toString());

        /*
         * Turn off Unicode support for index keys (this is a big win for load
         * rates since LUBM does not use Unicode data, but it has very little
         * effect on query rates since the only time we generate Unicode sort
         * keys is when resolving the Values in the queries to term identifiers
         * in the database).
         */
//        properties.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
        /*
         * Turn off the full text index unless it has been explicitly enabled.
         */
        if (properties.getProperty(Options.TEXT_INDEX) == null) {

            properties.setProperty(Options.TEXT_INDEX, "false");
            
        }

        /*
         * Set the branching factory.
         */
        properties.setProperty(IndexMetadata.Options.BTREE_BRANCHING_FACTOR, "64");

        /*
         * Turn on bloom filter for the SPO index (good up to ~2M index entries
         * for scale-up -or- for any size index for scale-out).
         */
        properties.setProperty(Options.BLOOM_FILTER, "true");

        /*
         * Turn off statement identifiers.
         */
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

        // Triples only.
        properties.setProperty(Options.QUADS, "false");

        /*
         * Turn off justifications (impacts only the load performance, but
         * it is a big impact and only required if you will be doing TM).
         */
        properties.setProperty(Options.JUSTIFY, "false");
        
        /*
         * Maximum #of subqueries to evaluate concurrently for the 1st join
         * dimension for native rules. Zero disables the use of an executor
         * service. One forces a single thread, but runs the subquery on the
         * executor service. N>1 is concurrent subquery evaluation.
         */
//        properties.setProperty(Options.MAX_PARALLEL_SUBQUERIES, "5");
        properties.setProperty(Options.MAX_PARALLEL_SUBQUERIES, "0");

//        /*
//         * Choice of the join algorithm.
//         * 
//         * false is pipeline.
//         * 
//         * true is nested.
//         */
//        properties.setProperty(Options.NESTED_SUBQUERY, "false");
        
        {
         
            /*
             * Note: The JDS and JDSTest properties are set in the individual config
             * files!!! The settings here only effect the LDS and EDS
             * configurations.
             */

            /*
             * Don't collect statistics from the OS for either the
             * IBigdataClient or the DataService.
             */
            properties
                    .setProperty(
                            IBigdataClient.Options.COLLECT_PLATFORM_STATISTICS,
                            "false");
            
//            /*
//             * Don't sample the various queues.
//             */
//            properties.setProperty(
//                    IBigdataClient.Options.COLLECT_QUEUE_STATISTICS, "false");
//
//            /*
//             * Don't run the httpd service.
//             */
//            properties.setProperty(IBigdataClient.Options.HTTPD_PORT, "-1");

            /*
             * Only one data service for the embedded data service.
             */
//            properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");
            properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "2");

            // Disable overflow of the live journal.
//            properties.setProperty(OverflowManager.Options.OVERFLOW_ENABLED,"false");

            // Disable index partition moves.
//            properties.setProperty(OverflowManager.Options.MAXIMUM_MOVES_PER_TARGET,"0");

            // Very small extent in order to provoke overflow.
//            properties.setProperty(Options.INITIAL_EXTENT, ""+Bytes.megabyte*50);
//            properties.setProperty(Options.MAXIMUM_EXTENT, ""+Bytes.megabyte*50);

            // Very small splits.
//            properties.setProperty(IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT, ""+1 * Bytes.kilobyte32);
//            properties.setProperty(IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT, ""+5 * Bytes.kilobyte32);

            // write retention queue (default is 500).
            properties.setProperty(
                    IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY,
                    "4000");

            if(false){ // custom OSP index override
                final String namespace = getNamespace();
                final String overrideValue = "200000";
                final String overrideProperty = com.bigdata.config.Configuration
                        .getOverrideProperty(
                                namespace + "." + SPORelation.NAME_SPO_RELATION
                                        + "." + SPOKeyOrder.OSP,
                                IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY);
                properties.setProperty(overrideProperty, overrideValue);
                System.err.println(overrideProperty + "=" + overrideValue);
            }
            if(false){ // custom POS index override
                final String namespace = getNamespace();
                final String overrideValue = "200000";
                final String overrideProperty = com.bigdata.config.Configuration
                        .getOverrideProperty(
                                namespace + "." + SPORelation.NAME_SPO_RELATION
                                        + "." + SPOKeyOrder.POS,
                                IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY);
                properties.setProperty(overrideProperty, overrideValue);
                System.err.println(overrideProperty + "=" + overrideValue);
            }
              
//
//            properties.setProperty(
//                    IndexMetadata.Options.BTREE_BRANCHING_FACTOR,
//                    "64");

            {
                /*
                 * Tweak the split points for the various indices to achieve
                 * 100M segments (based on U50 data set).
                 * 
                 * FIXME These changes should be applied to the triple store, but
                 * in a manner that permits explicit override.
                 */
                final String namespace = getNamespace();

                // turn on direct buffering for index segment nodes.
                properties.setProperty(Configuration.getOverrideProperty(
                        namespace,
                        IndexMetadata.Options.INDEX_SEGMENT_BUFFER_NODES),
                        "true");

                // statement indices (10x the default)
                properties.setProperty(Configuration.getOverrideProperty(namespace
                    + "." + SPORelation.NAME_SPO_RELATION,
                    IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT), ""
                    + (500 * Bytes.kilobyte32 * 10));
            
                properties.setProperty(Configuration.getOverrideProperty(namespace
                    + "."+  SPORelation.NAME_SPO_RELATION,
                    IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT), ""
                    + (1 * Bytes.megabyte32 * 10));

                // term2id index (5x the default).
                properties.setProperty(Configuration.getOverrideProperty(namespace
                        +"."+ LexiconRelation.NAME_LEXICON_RELATION
                        +"."+ LexiconKeyOrder.TERM2ID,
                        IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT), ""
                        + (500 * Bytes.kilobyte32 * 5));
                
                properties.setProperty(Configuration.getOverrideProperty(namespace
                    +"."+ LexiconRelation.NAME_LEXICON_RELATION
                    +"."+ LexiconKeyOrder.TERM2ID,
                    IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT), ""
                    + (1 * Bytes.megabyte32 * 5));

                // id2term index (2x the default)
                properties.setProperty(Configuration.getOverrideProperty(namespace
                        +"."+ LexiconRelation.NAME_LEXICON_RELATION
                        +"."+ LexiconKeyOrder.ID2TERM,
                        IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT), ""
                        + (500 * Bytes.kilobyte32 * 2));
                
                properties.setProperty(Configuration.getOverrideProperty(namespace
                    +"."+ LexiconRelation.NAME_LEXICON_RELATION
                    +"."+ LexiconKeyOrder.ID2TERM,
                    IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT), ""
                    + (1 * Bytes.megabyte32 * 2));
                
            }
            
        }

//        /*
//         * A large values works well for scale-up but you might need to reduce
//         * the read retention queue capacity since if you expect to have a large
//         * #of smaller indices open, e.g., for scale-out scenarios. Zero will
//         * disable the read-retention queue.
//         */
//        properties.setProperty(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY, "0");
        
        // may be used to turn off database-at-once closure during load.  
//        properties.setProperty(Options.COMPUTE_CLOSURE, "false");
        
        /*
         * May be used to turn off inference during query.
         * 
         * Note: This will cause ALL inferences to be filtered out!
         */
//        properties.setProperty(Options.INCLUDE_INFERRED, "false");
        
        /*
         * May be used to turn off query-time expansion of entailments such as
         * (x rdf:type rdfs:Resource) and owl:sameAs even through those
         * entailments were not materialized during forward closure.
         */
        properties.setProperty(Options.QUERY_TIME_EXPANDER, "false");

        /*
         * Note: LUBM uses blank nodes. Therefore re-loading LUBM will always
         * cause new statements to be asserted and result in the closure being
         * updated if it is recomputed. Presumably you can tell bigdata to store
         * the blank nodes and RIO to preserve them, but it does not seem to
         * work (RIO creates new blank nodes on reparse). Maybe this is a RIO
         * bug?
         */
//        properties.setProperty(Options.STORE_BLANK_NODES,"true");
        
        return properties;
        
    }
    
    /**
     * Create/re-open the repository.
     * <p>
     * If the backing database does not exist, then create it and create the
     * {@link AbstractTripleStore} on that database. When the backing database
     * is an {@link IBigdataFederation}, then you can either re-open an
     * existing federation or create one for the purposes of the test ({@link LDS},
     * {@link EDS}) or connect to an existing federation ({@link JDS} for
     * scale-out configurations).
     */
    public Repository create() {

        final IRepositoryLifeCycle lifeCycle = newLifeCycle();
        
        // open the database first.
        final IIndexManager indexManager = lifeCycle.open();

        final String namespace = getNamespace();
        
        /*
         * Create/re-open the triple store.
         */
        AbstractTripleStore tripleStore;
        {

            // locate the resource declaration (aka "open").
            tripleStore = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null) {

                // not declared, so create it now.
                
                System.out.println("Creating tripleStore: "+namespace);
                
                /*
                 * Fetch the properties.
                 * 
                 * Note: This grounds out at this.getProperties()
                 */
                final Properties properties = lifeCycle.getProperties();

                if (indexManager instanceof IBigdataFederation) {

                    tripleStore = new ScaleOutTripleStore(indexManager,
                            namespace, ITx.UNISOLATED, properties);

                } else {

                    tripleStore = new LocalTripleStore(indexManager, namespace,
                            ITx.UNISOLATED, properties);
                    
                    System.out.println("Opened tripleStore: "+namespace);
                    
                }

                // create the triple store.
                tripleStore.create();

            }

            /*
             * Dump some properties of interest.
             */
            {
                final Properties p = tripleStore.getProperties();
             
//                System.err.println(Options.INCLUDE_INFERRED + "="
//                        + p.getProperty(Options.INCLUDE_INFERRED));
//                
//                System.err.println(Options.QUERY_TIME_EXPANDER + "="
//                        + p.getProperty(Options.QUERY_TIME_EXPANDER));

                // iff LTS.  Other variants use data.dir.
                System.err.println(Options.FILE + "="
                        + p.getProperty(Options.FILE));

                System.err.println(Options.NESTED_SUBQUERY + "="
                        + p.getProperty(Options.NESTED_SUBQUERY));

              System.err.println(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY + "="
              + p.getProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY));

              System.err.println(IndexMetadata.Options.BTREE_BRANCHING_FACTOR+ "="
              + p.getProperty(IndexMetadata.Options.BTREE_BRANCHING_FACTOR));

//                System.err.println(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY + "="
//                        + p.getProperty(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY));
              
                System.err.println(Options.CHUNK_CAPACITY + "="
                        + p.getProperty(Options.CHUNK_CAPACITY));
                
                System.err.println(Options.CHUNK_TIMEOUT
                        + "="
                        + p.getProperty(Options.CHUNK_TIMEOUT,
                                Options.DEFAULT_CHUNK_TIMEOUT));

                System.err.println(IBigdataClient.Options.CLIENT_RANGE_QUERY_CAPACITY
                        + "="
                        + p.getProperty(IBigdataClient.Options.CLIENT_RANGE_QUERY_CAPACITY,
                                IBigdataClient.Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY));

                System.err.println(Options.FULLY_BUFFERED_READ_THRESHOLD
                        + "="
                        + p.getProperty(Options.FULLY_BUFFERED_READ_THRESHOLD,
                                Options.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD));

                System.err.println(Options.MAX_PARALLEL_SUBQUERIES + "="
                        + p.getProperty(Options.MAX_PARALLEL_SUBQUERIES));
                
                System.err.println(Options.QUERY_TIME_EXPANDER+ "="
                        + p.getProperty(Options.QUERY_TIME_EXPANDER));
                
                System.err.println("bloomFilterFactory="
                        + tripleStore.getSPORelation().getPrimaryIndex()
                                .getIndexMetadata().getBloomFilterFactory());
                
            }

            // close now that it exists. it will be re-opened on demand.
            lifeCycle.close(indexManager);
            
        }
        
        return new BigdataRepository(lifeCycle);
        
    }

    /**
     * Concrete implementations implement this methods.
     */
    abstract public IRepositoryLifeCycle newLifeCycle();
    
    /**
     * Abstraction to manage open and close of the backing database for a
     * {@link Journal} and the various kinds of {@link IBigdataFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IRepositoryLifeCycle<T extends IIndexManager> {
        
        /**
         * Open the backing database.
         * 
         * @return the {@link IIndexManager}
         */
        T open();

        /**
         * Close the backing database.
         */
        void close(T indexManager);
        
        /**
         * The properties used to initialize the environment. This can be used
         * to pass additional parameters through to the
         * {@link BigdataRepository}.
         */
        Properties getProperties();
        
    }
    
    /**
     * Additional options specific to this test harness.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends com.bigdata.rdf.sail.BigdataSail.Options {

        /**
         * The namespace of the kb (multiple kbs can be stored in a single
         * database instance (default {@value #DEFAULT_NAMESPACE}).
         */
        String NAMESPACE = "namespace";

        String DEFAULT_NAMESPACE = "kb";
        
        /**
         * When <code>true</code> inferences are included in query results
         * (this effects the value of Sesame parameter by the same name). This
         * may be used to turn off inference in order to see the results of a
         * pure access path read with either native rules or Sesame 2
         * evaluation. The value DOES NOT effect the behavior when computing
         * closure.
         * <p>
         * Note: This is applied by explicitly setting the [includedInferred]
         * argument when invoking
         * {@link BigdataSailConnection#evaluate(org.openrdf.query.algebra.TupleExpr, org.openrdf.query.Dataset, org.openrdf.query.BindingSet, boolean)}
         */
        String INCLUDE_INFERRED = "includeInferred";

        String DEFAULT_INCLUDE_INFERRED = "true";

        /**
         * When <code>true</code>, the closure of the data set will be
         * computed. Note that the closure will be computed ANYWAY if the
         * {@link BigdataSail} is configured for incremental truth maintenance.
         * 
         * @see BigdataSail.Options#TRUTH_MAINTENANCE
         */
        String COMPUTE_CLOSURE = "computeClosure";

        String DEFAULT_COMPUTE_CLOSURE = "true";
        
        /**
         * When <code>true</code>, the specified ontology and data files will
         * be loaded. This can be disabled if you just want to compute the
         * closure of the database.
         */
        String LOAD_DATA = "loadData";

        String DEFAULT_LOAD_DATA = "true";

        /**
         * When <code>true</code> the triple store will be verified by
         * re-parsing the ontology and data file(s) and verifying that each
         * statement encountered in those files exists as an explicit statement
         * in the knowledge base.
         * <p>
         * Note: LUBM uses blank nodes. The verification algorithm does not
         * handle blank nodes in any special manner, therefore both the blank
         * nodes themselves and the statements made using blank nodes will show
         * up as "unknown".
         */
        String VERIFY_DATA = "verifyData";
        
        String DEFAULT_VERIFY_DATA = "false";
        
        /*
         * Options for the ConcurrentDataLoader.
         */
        
        /**
         * #of threads to use on each client.
         */
        String CONCURRENT_DATA_LOADER_NTHREADS  = "concurrentDataLoader.nthreads";
        /**
         * #of clients to use.
         */
        String CONCURRENT_DATA_LOADER_NCLIENTS  = "concurrentDataLoader.nclients";
        /**
         * The client index for this client in [0:nclients-1].
         */
        String CONCURRENT_DATA_LOADER_CLIENTNUM = "concurrentDataLoader.clientNum";
        
        /**
         * The buffer capacity for the concurrentdataload
         */
        String CONCURRENT_DATA_LOADER_BUFFER_CAPACITY = "concurrentDataLoader.bufferCapacity";

        String DEFAULT_CONCURRENT_DATA_LOADER_NTHREADS = "10";
        String DEFAULT_CONCURRENT_DATA_LOADER_NCLIENTS = "1";
        String DEFAULT_CONCURRENT_DATA_LOADER_CLIENTNUM = "0";
        String DEFAULT_CONCURRENT_DATA_LOADER_BUFFER_CAPACITY = "100000";
        
        /*
         * Federation options (some are JDS specific).
         */
        
        /**
         * The minimum #of data services that must be online (discovered) before
         * the test can proceed (default {@value #DEFAULT_MIN_DATA_SERVICES}).
         * 
         * @see JDS
         */
        String MIN_DATA_SERVICES = "minDataServices";

        String DEFAULT_MIN_DATA_SERVICES = "2";

        /**
         * Option may be used to force overflow of all data services in the
         * federation after load/load+closure.
         */
        String FORCE_OVERFLOW_AFTER_LOAD = "forceOverflowAfterLoad";
        
        String DEFAULT_FORCE_OVERFLOW_AFTER_LOAD = "false";
        
    }
    
}
