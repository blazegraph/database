/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jan 17, 2009
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Callable;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.axioms.RdfsAxioms;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZooQueue;

/**
 * Distributed bulk loader for RDF data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFDataLoadMaster implements Callable<Void> {

    final protected static Logger log = Logger
            .getLogger(RDFDataLoadMaster.class);

    final protected static boolean INFO = log.isInfoEnabled();

    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * {@link Configuration} options for the {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

        /**
         * The component (namespace for the configuration options).
         */
        String COMPONENT = RDFDataLoadMaster.class.getName();

        /**
         * The KB namespace.
         */
        String NAMESPACE = "namespace";

        /**
         * The directory from which the data will be read.
         */
        String DATA_DIR = "dataDir";

        /**
         * A file or directory whose data will be loaded into the KB when it is
         * created. If it is a directory, then all data in that directory will
         * be loaded. Unlike the distributed bulk load, the file or directory
         * MUST be readable by the master and the data in this file and/or
         * directory are NOT deleted after they have been loaded.
         * <p>
         * Note: This is intended for the one-time load of ontologies pertaining
         * to the data to be loaded. If you need to do additional non-bulk data
         * loads you can always use the {@link BigdataSail}.
         */
        String ONTOLOGY = "ontology";

        /**
         * #of clients to use.
         */
        String NCLIENTS = "nclients";

        /**
         * #of threads to use on each client.
         */
        String NTHREADS = "nthreads";

        /**
         * The buffer capacity for parsed RDF statements.
         */
        String BUFFER_CAPACITY = "bufferCapacity";

        /**
         * When <code>true</code>, the master will create the
         * {@link ITripleStore} identified by {@link #NAMESPACE } if it does not
         * exist.
         */
        String CREATE = "create";

        /**
         * When <code>true</code>, the data files will be loaded. This can be
         * disabled if you just want to compute the closure of the database.
         */
        String LOAD_DATA = "loadData";

        /**
         * When <code>true</code>, the closure of the data set will be
         * computed.
         * 
         * @todo Note that the closure will be computed ANYWAY if the
         *       {@link BigdataSail} is configured for incremental truth
         *       maintenance. (Create w/o incremental TM).
         * 
         * @see BigdataSail.Options#TRUTH_MAINTENANCE
         */
        String COMPUTE_CLOSURE = "computeClosure";

        /**
         * When <code>true</code>, the data files will be deleted as they are
         * consumed. (They will only be deleted once the data from the file are
         * known to be restart safe in the {@link ITripleStore}.)
         */
        String DELETE_AFTER = "deleteAfter";

        // more properly belongs in a federation utility class.
        // /**
        // * Option may be used to force overflow of all data services in the
        // * federation after load/load+closure.
        // */
        // String FORCE_OVERFLOW_AFTER_LOAD = "forceOverflowAfterLoad";
        //        
        // String DEFAULT_FORCE_OVERFLOW_AFTER_LOAD = "false";

    }

    /**
     * The job description for an {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JobState implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -7097810235721797668L;

        /**
         * The namespace of the {@link ITripleStore} into which the data will be
         * loaded (must exist).
         */
        public final String namespace;

        /**
         * The directory from which files will be read.
         * 
         * @todo is this needed if we use a {@link ZooQueue} integration?
         */
        public final File dataDir;

        /**
         * The file or directory from which files will be loaded when the
         * {@link ITripleStore} is first created.
         * 
         * @see ConfigurationOptions#ONTOLOGY
         */
        public final File ontology;

        /**
         * The #of clients. Each client will be assigned to run on some
         * {@link IDataService}. Each client may run multiple threads.
         * <p>
         * Note: When loading data from the local file system a client MUST be
         * created on an {@link IDataService} on each host in order to read data
         * from its local file system. This is not an issue when reading from a
         * shared volume.
         */
        public final int nclients;

        /**
         * The #of concurrent threads to use to load the data. A single thread
         * will be used to scan the file system, but this many threads will be
         * used to read, parse, and write the data onto the {@link ITripleStore}.
         */
        public final int nthreads;

        /**
         * The capacity of the buffers used to hold the parsed RDF data.
         */
        public final int bufferCapacity;

        /**
         * When <code>true</code>, the master will create the
         * {@link ITripleStore} identified by {@link #namespace} if it does not
         * exist.
         */
        public final boolean create;

        /**
         * When <code>true</code>, the clients will load data.
         */
        public final boolean loadData;

        /**
         * When <code>true</code>, the closure of the data set will be
         * computed once all data have been loaded.
         */
        public final boolean computeClosure;

        /**
         * When <code>true</code>, the each data file will be deleted once
         * its data has been loaded into the {@link ITripleStore}.
         */
        public final boolean deleteAfter;

        // @todo config (parser verifies source data).
        final public boolean verifyRDFSourceData = false;

        // @todo conig (default format assumed when file ext is unknown).
        // @todo serializable?
        final public RDFFormat fallback = RDFFormat.RDFXML;

        public String toString() {

            return getClass().getName()
                    + //
                    "{ " + ConfigurationOptions.NAMESPACE
                    + "="
                    + namespace
                    + //
                    ", " + ConfigurationOptions.DATA_DIR
                    + "="
                    + dataDir
                    + //
                    ", " + ConfigurationOptions.NCLIENTS
                    + "="
                    + nclients
                    + //
                    ", " + ConfigurationOptions.NTHREADS
                    + "="
                    + nthreads
                    + //
                    ", " + ConfigurationOptions.BUFFER_CAPACITY
                    + "="
                    + bufferCapacity
                    + //
                    ", " + ConfigurationOptions.CREATE + "="
                    + create
                    + //
                    ", " + ConfigurationOptions.LOAD_DATA + "="
                    + loadData
                    + //
                    ", " + ConfigurationOptions.COMPUTE_CLOSURE + "="
                    + computeClosure
                    + //
                    ", " + ConfigurationOptions.DELETE_AFTER + "="
                    + deleteAfter + //
                    "}";

        }

        /**
         * @param configuration
         * @throws ConfigurationException
         */
        public JobState(final Configuration config)
                throws ConfigurationException {

            namespace = (String) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NAMESPACE, String.class);

            dataDir = (File) config.getEntry(ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.DATA_DIR, File.class);

            ontology = (File) config
                    .getEntry(ConfigurationOptions.COMPONENT,
                            ConfigurationOptions.ONTOLOGY, File.class, null/* defaultValue */);

            nclients = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NCLIENTS, Integer.TYPE);

            nthreads = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NTHREADS, Integer.TYPE);

            bufferCapacity = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.BUFFER_CAPACITY, Integer.TYPE);

            create = (Boolean) config.getEntry(ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.CREATE, Boolean.TYPE);

            loadData = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.LOAD_DATA, Boolean.TYPE);

            computeClosure = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.COMPUTE_CLOSURE, Boolean.TYPE);

            deleteAfter = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.DELETE_AFTER, Boolean.TYPE);

        }

    }

    protected final JiniFederation fed;

    public final JobState jobState;

    public RDFDataLoadMaster(final JiniFederation fed)
            throws ConfigurationException {

        this.fed = fed;

        jobState = new JobState(fed.getClient().getConfiguration());

    }

    public Void call() throws Exception {

        // open/create the kb.
        final AbstractTripleStore tripleStore = openTripleStore();

        showProperties(tripleStore);

        /*
         * FIXME start clients (this is being used with lubm as the source for
         * now and needs to be generalized for other sources and other data flow
         * patterns).
         * 
         * @todo there need to be hooks for building queues from the file
         * system, zookeeper, etc.
         */

        throw new UnsupportedOperationException();

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
    protected AbstractTripleStore openTripleStore() {

        /*
         * Create/re-open the triple store.
         */
        AbstractTripleStore tripleStore;

        // locate the resource declaration (aka "open").
        tripleStore = (AbstractTripleStore) fed.getResourceLocator().locate(
                jobState.namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            /*
             * Does not exist.
             */

            if (!jobState.create) {

                throw new RuntimeException("Does not exist: "
                        + jobState.namespace);

            }

            // create kb.
            tripleStore = createTripleStore();

            // load any one time files.
            try {

                loadOntology(tripleStore);
                
            } catch (Exception ex) {
                
                throw new RuntimeException("Could not load: "
                        + jobState.ontology, ex);
                
            }

        } else {

            if (INFO)
                log.info("Opened tripleStore: " + jobState.namespace);

        }
        
        return tripleStore;

    }

    /**
     * Create the {@link ITripleStore} specified by
     * {@link ConfigurationOptions#NAMESPACE}.
     * 
     * @return The {@link ITripleStore}
     */
    protected AbstractTripleStore createTripleStore() {

        if (INFO)
            log.info("Creating tripleStore: " + jobState.namespace);

        AbstractTripleStore tripleStore = new ScaleOutTripleStore(fed,
                jobState.namespace, ITx.UNISOLATED, fed.getClient()
                        .getProperties());

        // create the triple store.
        tripleStore.create();

        if (INFO)
            log.info("Created tripleStore: " + jobState.namespace);

        return tripleStore;

    }

    /**
     * Loads the file or directory specified by
     * {@link ConfigurationOptions#ONTOLOGY} into the {@link ITripleStore}
     * 
     * @throws IOException
     */
    protected void loadOntology(final AbstractTripleStore tripleStore)
            throws IOException {

        if(INFO)
            log.info("Loading ontology: "+jobState.ontology);
        
        tripleStore.getDataLoader().loadFiles(jobState.ontology/* file */,
                jobState.ontology.getPath()/* baseURI */,
                null/* rdfFormat */, null/* filter */);

        if(INFO)
            log.info("Loaded ontology: "+jobState.ontology);

    }

    /**
     * Dump some properties of interest.
     */
    protected void showProperties(final AbstractTripleStore tripleStore) {

        final Properties p = tripleStore.getProperties();

        // System.err.println(Options.INCLUDE_INFERRED + "="
        // + p.getProperty(Options.INCLUDE_INFERRED));
        //                        
        // System.err.println(Options.QUERY_TIME_EXPANDER + "="
        // + p.getProperty(Options.QUERY_TIME_EXPANDER));

        // iff LTS. Other variants use data.dir.
        System.err.println(Options.FILE + "=" + p.getProperty(Options.FILE));

        System.err.println(Options.NESTED_SUBQUERY + "="
                + p.getProperty(Options.NESTED_SUBQUERY));

        System.err
                .println(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY
                        + "="
                        + p
                                .getProperty(IndexMetadata.Options.DEFAULT_BTREE_READ_RETENTION_QUEUE_CAPACITY));

        System.err.println(Options.CHUNK_CAPACITY + "="
                + p.getProperty(Options.CHUNK_CAPACITY));

        System.err.println(Options.CHUNK_TIMEOUT
                + "="
                + p.getProperty(Options.CHUNK_TIMEOUT,
                        Options.DEFAULT_CHUNK_TIMEOUT));

        System.err
                .println(IBigdataClient.Options.CLIENT_RANGE_QUERY_CAPACITY
                        + "="
                        + p
                                .getProperty(
                                        IBigdataClient.Options.CLIENT_RANGE_QUERY_CAPACITY,
                                        IBigdataClient.Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY));

        System.err.println(Options.FULLY_BUFFERED_READ_THRESHOLD
                + "="
                + p.getProperty(Options.FULLY_BUFFERED_READ_THRESHOLD,
                        Options.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD));

        System.err.println(Options.MAX_PARALLEL_SUBQUERIES + "="
                + p.getProperty(Options.MAX_PARALLEL_SUBQUERIES));

//        System.err.println(BigdataSail.Options.QUERY_TIME_EXPANDER + "="
//                + p.getProperty(BigdataSail.Options.QUERY_TIME_EXPANDER));

        System.err.println("bloomFilterFactory="
                + tripleStore.getSPORelation().getSPOIndex().getIndexMetadata()
                        .getBloomFilterFactory());

    }

    /**
     * Return the {@link Properties} that will be used by
     * {@link #createTripleStore()}. The {@link AbstractTripleStore} will
     * remember the properties with which it was created and use those values
     * each time it is re-opened. The properties are stored in the global row
     * store for the backing {@link IIndexManager}.
     * 
     * @param namespace
     *            The namespace for the {@link ITripleStore}.
     * 
     * @todo most of this should be read from the {@link Configuration} using
     *       the namespace of either the {@link ITripleStore} to be created or
     *       the client.
     * 
     * @todo some of these properties are in the bigdata-sails module. to avoid
     *       a compile time dependency those options are given using literals
     *       rather than symbolic constants.
     */
    protected Properties getProperties(final String namespace) {

        // pick up system properties as defaults.
//        final Properties properties = new Properties(System.getProperties());
        // do not pick up system properties as defaults.
//         final Properties properties = new Properties();

        /*
         * Pick up properties configured for the client as defaults.
         * 
         * You can specify those properties using NV[] for the component that is
         * executing the master.
         */
        final Properties properties = fed.getClient().getProperties();

        /*
         * When "true", the store will perform incremental closure as the data
         * are loaded. When "false", the closure will be computed after all data
         * are loaded. (Actually, since we are not loading through the SAIL
         * making this true does not cause incremental TM but it does disable
         * closure, so "false" is what you need here).
         */
        properties.setProperty(
                "com.bigdata.rdf.sail.truthMaintenance", "false"
//                BigdataSail.Options.TRUTH_MAINTENANCE, "false"
                );

        /*
         * Enable rewrites of high-level queries into native rules (native JOIN
         * execution). (Can be changed without re-loading the data to compare
         * the performance of the Sesame query evaluation against using the
         * native rules to perform query evaluation.)
         */
//        properties.setProperty(BigdataSail.Options.NATIVE_JOINS, "true");
        properties.setProperty("com.bigdata.rdf.sail.nativeJoins","true");

        /*
         * May be used to turn off inference during query.
         * 
         * Note: This will cause ALL inferences to be filtered out!
         */
        // properties.setProperty(Options.INCLUDE_INFERRED, "false");
        /*
         * May be used to turn off query-time expansion of entailments such as
         * (x rdf:type rdfs:Resource) and owl:sameAs even through those
         * entailments were not materialized during forward closure.
         */
        // Note: disables the backchainer!
        properties.setProperty("com.bigdata.rdf.sail.queryTimeExpander","false");
//      properties.setProperty(BigdataSail.Options.QUERY_TIME_EXPANDER, "false");

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
         */
        properties
                .setProperty(Options.AXIOMS_CLASS, RdfsAxioms.class.getName());
        // properties.setProperty(Options.AXIOMS_CLASS,NoAxioms.class.getName());

        /*
         * Produce a full closure (all entailments) so that the backward chainer
         * is always a NOP. Note that the configuration properties are stored in
         * the database (in the global row store) so you always get exactly the
         * same configuration that you created when reopening a triple store.
         */
        // properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,
        // "true");
        // properties.setProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES,
        // "true");
        /*
         * Additional owl inferences. LUBM only both inverseOf and
         * TransitiveProperty of those that we support (owl:sameAs,
         * owl:inverseOf, owl:TransitiveProperty), but not owl:sameAs.
         */
        properties.setProperty(Options.FORWARD_CHAIN_OWL_INVERSE_OF, "true");
        properties.setProperty(Options.FORWARD_CHAIN_OWL_TRANSITIVE_PROPERY,
                "true");

        // Note: FastClosure is the default.
        // properties.setProperty(Options.CLOSURE_CLASS,
        // FullClosure.class.getName());

        /*
         * Various things that effect native rule execution.
         */
        // properties.setProperty(Options.FORCE_SERIAL_EXECUTION, "false");
        // properties.setProperty(Options.MUTATION_BUFFER_CAPACITY, "20000");
        properties.setProperty(Options.CHUNK_CAPACITY, "100");
        // properties.setProperty(Options.QUERY_BUFFER_CAPACITY, "10000");
        // properties.setProperty(Options.FULLY_BUFFERED_READ_THRESHOLD,
        // "10000");

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
        properties.setProperty(com.bigdata.rdf.store.DataLoader.Options.COMMIT,
                DataLoader.CommitEnum.None.toString());

        /*
         * Turn off Unicode support for index keys (this is a big win for load
         * rates since LUBM does not use Unicode data, but it has very little
         * effect on query rates since the only time we generate Unicode sort
         * keys is when resolving the Values in the queries to term identifiers
         * in the database).
         */
        // properties.setProperty(Options.COLLATOR,
        // CollatorEnum.ASCII.toString());
        /*
         * Turn off the full text index unless it has been explicitly enabled.
         */
        if (properties.getProperty(Options.TEXT_INDEX) == null) {

            properties.setProperty(Options.TEXT_INDEX, "false");

        }

        /*
         * Turn on bloom filter for the SPO index (good up to ~2M index entries
         * for scale-up -or- for any size index for scale-out).
         */
        properties.setProperty(Options.BLOOM_FILTER, "true");

        /*
         * Turn off statement identifiers.
         */
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

        /*
         * Turn off justifications (impacts only the load performance, but it is
         * a big impact and only required if you will be doing TM).
         */
        properties.setProperty(Options.JUSTIFY, "false");

        /*
         * Maximum #of subqueries to evaluate concurrently for the 1st join
         * dimension for native rules. Zero disables the use of an executor
         * service. One forces a single thread, but runs the subquery on the
         * executor service. N>1 is concurrent subquery evaluation.
         */
        // properties.setProperty(Options.MAX_PARALLEL_SUBQUERIES, "5");
        properties.setProperty(Options.MAX_PARALLEL_SUBQUERIES, "0");

        /*
         * Choice of the join algorithm.
         * 
         * false is pipeline, which scales-out.
         * 
         * true is nested, which is also the default right now but does not
         * scale-out.
         */
        properties.setProperty(Options.NESTED_SUBQUERY, "false");

        {
            /*
             * Tweak the split points for the various indices to achieve 100M
             * segments (based on U50 data set).
             * 
             * FIXME These changes should be applied to the triple store, but in
             * a manner that permits explicit override. They result in good
             * sizes for the index partitions.
             */

            // turn on direct buffering for index segment nodes.
            properties.setProperty(com.bigdata.config.Configuration
                    .getOverrideProperty(namespace,
                            IndexMetadata.Options.INDEX_SEGMENT_BUFFER_NODES),
                    "true");

            // statement indices (10x the default)
            properties
                    .setProperty(
                            com.bigdata.config.Configuration
                                    .getOverrideProperty(
                                            namespace
                                                    + "."
                                                    + SPORelation.NAME_SPO_RELATION,
                                            IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT),
                            "" + (500 * Bytes.kilobyte32 * 10));

            properties
                    .setProperty(
                            com.bigdata.config.Configuration
                                    .getOverrideProperty(
                                            namespace
                                                    + "."
                                                    + SPORelation.NAME_SPO_RELATION,
                                            IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT),
                            "" + (1 * Bytes.megabyte32 * 10));

            // term2id index (5x the default).
            properties
                    .setProperty(
                            com.bigdata.config.Configuration
                                    .getOverrideProperty(
                                            namespace
                                                    + "."
                                                    + LexiconRelation.NAME_LEXICON_RELATION
                                                    + "."
                                                    + LexiconKeyOrder.TERM2ID,
                                            IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT),
                            "" + (500 * Bytes.kilobyte32 * 5));

            properties
                    .setProperty(
                            com.bigdata.config.Configuration
                                    .getOverrideProperty(
                                            namespace
                                                    + "."
                                                    + LexiconRelation.NAME_LEXICON_RELATION
                                                    + "."
                                                    + LexiconKeyOrder.TERM2ID,
                                            IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT),
                            "" + (1 * Bytes.megabyte32 * 5));

            // id2term index (2x the default)
            properties
                    .setProperty(
                            com.bigdata.config.Configuration
                                    .getOverrideProperty(
                                            namespace
                                                    + "."
                                                    + LexiconRelation.NAME_LEXICON_RELATION
                                                    + "."
                                                    + LexiconKeyOrder.ID2TERM,
                                            IndexMetadata.Options.SPLIT_HANDLER_MIN_ENTRY_COUNT),
                            "" + (500 * Bytes.kilobyte32 * 2));

            properties
                    .setProperty(
                            com.bigdata.config.Configuration
                                    .getOverrideProperty(
                                            namespace
                                                    + "."
                                                    + LexiconRelation.NAME_LEXICON_RELATION
                                                    + "."
                                                    + LexiconKeyOrder.ID2TERM,
                                            IndexMetadata.Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT),
                            "" + (1 * Bytes.megabyte32 * 2));

        }

        // /*
        // * This buffers the mutable B+Trees. A large value is good (the
        // default
        // * is a large value).
        // */
        // properties.setProperty(
        // IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY, "10000");

        // may be used to turn off database-at-once closure during load.
        // properties.setProperty(Options.COMPUTE_CLOSURE, "false");

        /*
         * Note: LUBM uses blank nodes. Therefore re-loading LUBM will always
         * cause new statements to be asserted and result in the closure being
         * updated if it is recomputed. Presumably you can tell bigdata to store
         * the blank nodes and RIO to preserve them, but it does not seem to
         * work (RIO creates new blank nodes on reparse). Maybe this is a RIO
         * bug?
         */
        // properties.setProperty(Options.STORE_BLANK_NODES,"true");

        return properties;

    }

    /**
     * Task reads files from the file system, loads them into an
     * {@link ITripleStore}, and then deletes the files if they have been
     * loaded successfully. This is a non-transactional bulk load using
     * unisolated writes. If the writes succeed then the client knows that the
     * data are on stable storage and may safely delete the source files. This
     * task may be used in conjunction with any process when can write files
     * into a known directory on the hosts of a cluster. The task will continue
     * until cancelled.
     * <p>
     * Note: Counters reporting the progress of this task will be attached to
     * the data service on which this task is executing.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo report counters to federation.
     */
    abstract public static class RDFFileLoadTask implements Callable<Void>,
            Serializable, IDataServiceAwareProcedure {

        /**
         * The {@link DataService} on which the client is executing. This is set
         * automatically by the {@link DataService} when it notices that this
         * class implements the {@link IDataServiceAwareProcedure} interface.
         */
        private transient DataService dataService;

        protected final JobState jobState;
        
        protected final int clientNum;

        public String toString() {

            return getClass().getName() + "{clientNum=" + clientNum + "}";

        }

        public RDFFileLoadTask(final JobState jobState, final int clientNum) {

            this.jobState = jobState;
            
            this.clientNum = clientNum;

        }

        public void setDataService(final DataService dataService) {

            this.dataService = dataService;

        }

        /**
         * Return the {@link DataService} on which this task is executing.
         * 
         * @throws IllegalStateException
         *             if the task is not executing on a {@link DataService}.
         */
        public DataService getDataService() {
            
            if (dataService == null)
                throw new IllegalStateException();
            
            return dataService;
            
        }
        
        /**
         * The federation object used by the {@link DataService} on which this
         * task is executing.
         */
        public JiniFederation getFederation() {

            return (JiniFederation) getDataService().getFederation();

        }
        
        /**
         * {@link ConcurrentDataLoader}
         */
        public Void call() throws Exception {

            if (dataService == null)
                throw new IllegalStateException();

            final JiniFederation fed = (JiniFederation) dataService
                    .getFederation();

            final AbstractTripleStore tripleStore = (AbstractTripleStore) fed
                    .getResourceLocator().locate(jobState.namespace,
                            ITx.UNISOLATED);

            loadData(tripleStore);
            
            return null;

        }

        /**
         * Hook to load data is invoked once by {@link #call()}. The
         * implementation must run until no more data (if that condition can be
         * detected) or until the job is cancelled.
         * 
         * @param tripleStore
         *            Where to put the data.
         * 
         * @throws InterruptedException
         *             if interrupted (job cancelled).
         * @throws Exception
         *             if something else goes wrong.
         * 
         * FIXME Offer an implementation which reads from a known directory and
         * runs until cancelled so that any data dropped into the file system
         * will be bulk loaded.
         */
        abstract protected void loadData(AbstractTripleStore tripleStore)
                throws InterruptedException, Exception;

        /**
         * Recursively removes any files and subdirectories and then removes the
         * file (or directory) itself.
         * 
         * @param file
         *            A file or directory.
         */
        protected void recursiveDelete(final File file) {

            if (file.isDirectory()) {

                final File[] children = file.listFiles();

                for (int i = 0; i < children.length; i++) {

                    recursiveDelete(children[i]);

                }

            }

            if (INFO)
                log.info("Removing: " + file);

            if (!file.delete())
                log.warn("Could not remove: " + file);

        }

    }
    
}

/*
 * @todo There should be someway to unify jini and the namespace hierarchy stuff
 * used for relations and the metadata index.
 */

// /**
// * Exposes the jini {@link Configuration} interface but is namespace aware
// * and so benefits from inherited properties.
// *
// * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
// Thompson</a>
// * @version $Id$
// *
// * @see com.bigdata.config.Configuration
// */
// private class NamespaceAwareConfiguration implements Configuration {
//
//    
// public Object getEntry(String arg0, String arg1, Class arg2)
// throws ConfigurationException {
// return null;
// }
//
// public Object getEntry(String arg0, String arg1, Class arg2, Object arg3)
// throws ConfigurationException {
// return null;
// }
//
// public Object getEntry(String arg0, String arg1, Class arg2,
// Object arg3, Object arg4) throws ConfigurationException {
// return null;
// }
//
//}
