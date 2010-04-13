/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on May 23, 2007
 */

package com.bigdata.rdf.stress;

import info.aduna.iteration.CloseableIteration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.CognitiveWeb.util.PropertyUtil;
import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.load.ConcurrentDataLoader;
import com.bigdata.rdf.load.FileSystemLoader;
import com.bigdata.rdf.load.RDFLoadTaskFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.relation.RelationSchema;
import com.bigdata.resources.OverflowManager;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceClient;
import com.bigdata.service.jini.util.JiniServicesHelper;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.NV;

/**
 * Parameterized test to verify load, closure, restart, and query for each of
 * the database deployment models. The test has as inputs a set of properties, a
 * set of RDF data file(s), and a set of (SPARQL) query files. This can be used
 * to readily verify consistency and cross-check performance for all tripleStore
 * modes on any dataset. There is both {@link #main(String[])} for simple runs
 * of a single condition and {@link GenerateExperiment#main(String[])}, which
 * will generate an XML serialization of the inputs required to run a set of
 * conditions using the {@link ExperimentDriver}.
 * 
 * @todo The test should report out the configured properties, load (time, #of
 *       statements), closure (time, #of statements), and query times (by query
 *       and aggregate).
 *       <p>
 *       and also whether or not the same #of triples was loaded, #of triples
 *       was inferred by closure (if enabled by the properties), and #of answers
 *       was produced by query.
 * 
 * @todo could note various OS counters as well in the results.
 * 
 * @todo its ok to bundle the generated LUBM U1 data, but is it ok to bundle the
 *       ontology?
 * 
 * @todo test full text search features. E.g., the EOSSYS queries with LIMIT and
 *       BNS#SEARCH.
 * 
 * @todo Some test options would include: overflow during data load (by
 *       splitting the files to be loaded into two groups) or after data load so
 *       that closure reads from complex views.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadClosureAndQueryTest implements IComparisonTest {

    protected static final Logger log = Logger.getLogger(LoadClosureAndQueryTest.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * 
     */
    public LoadClosureAndQueryTest() {

    }
    
    /**
     * (Re-)open a SAIL backed by persistent data stored in an
     * {@link IBigdataFederation}.
     * 
     * @param fed
     *            The federation.
     * @param namespace
     *            The namespace of the triple store.
     * @param timestamp
     *            The timestamp of the view.
     * 
     * @return The SAIL.
     */
    public BigdataSail getSail(IBigdataFederation fed, String namespace,
            long timestamp) {

        ScaleOutTripleStore tripleStore = (ScaleOutTripleStore) fed
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null) {

            if (timestamp == ITx.UNISOLATED) {

                // create a new triple store.

                System.out.println("Creating tripleStore: namespace="
                        + namespace);

                tripleStore = new ScaleOutTripleStore(fed, namespace,
                        timestamp, fed.getClient().getProperties());

                tripleStore.create();

            } else {

                throw new RuntimeException("No such triple store: namespace="
                        + namespace + ", timestamp=" + timestamp);

            }

        }

        return new BigdataSail(tripleStore);

    }
    
    /**
     * (Re-)open a SAIL backed by persistent data stored on a {@link Journal}.
     * 
     * @param filename
     *            The name of the backing file for the {@link Journal}.
     * @param namespace
     *            The namespace of the triple store.
     * @param timestamp
     *            The timestamp of the view.
     * @param properties
     *            The properties that will be used to create a new SAIL.
     * 
     * @return The SAIL.
     */
    public BigdataSail getSail(String namespace, long timestamp,
            Properties properties) {

        final Journal journal = new Journal(properties);

        LocalTripleStore tripleStore = (LocalTripleStore) journal
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null) {

            if (timestamp == ITx.UNISOLATED) {

                // create a new triple store.

                System.out.println("Creating tripleStore: namespace="
                        + namespace);

                tripleStore = new LocalTripleStore(journal, namespace,
                        timestamp, properties);

                tripleStore.create();

            } else {

                throw new RuntimeException("No such triple store: namespace="
                        + namespace + ", timestamp=" + timestamp);
                
            }
            
        }
        
        return new BigdataSail(tripleStore);
        
    }

    /**
     * Return the properties associated with the {@link AbstractTripleStore}
     * backing the {@link BigdataSail}.
     * 
     * @param namespace
     *            The sail.
     *            
     * @return The persistent properties.
     */
    public Properties getProperties(BigdataSail sail) {

        return getProperties(sail.getDatabase().getIndexManager(), sail
                .getDatabase().getNamespace());
        
    }
    
    /**
     * Return the properties associated with the given namespace.
     * 
     * @param indexManager
     *            Use {@link BigdataSail#getDatabase()} and then
     *            {@link AbstractTripleStore#getIndexManager()}.
     * @param namespace
     *            The namespace of a locatable resource such as an
     *            {@link AbstractTripleStore}, {@link SPORelation} or
     *            {@link LexiconRelation}.
     * 
     * @return The persistent properties.
     */
    protected Properties getProperties(IIndexManager indexManager,
            String namespace) {

        Map<String, Object> map = indexManager.getGlobalRowStore().read(
                RelationSchema.INSTANCE, namespace);

        Properties properties = new Properties();

        properties.putAll(map);

        return properties;

    }

    /**
     * List out all properties.
     * 
     * @param p
     *            The properties.
     */
    protected static void showProperties(Properties p) {
        
        // sorted collection.
        final TreeMap<String/*name*/, Object/*val*/> map = new TreeMap<String,Object>();

        // put into alpha order.
        for(Map.Entry<Object,Object> entry : p.entrySet()) {
            
            map.put(entry.getKey().toString(), entry.getValue());
            
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {

            System.out.println(entry.getKey() + "=" + entry.getValue());

        }
        
    }

    /**
     * Show some properties of interest.
     */
    public static void showInterestingProperties(AbstractTripleStore tripleStore) {

        final Properties p = tripleStore.getProperties();
         
//            System.err.println(Options.INCLUDE_INFERRED + "="
//                    + p.getProperty(Options.INCLUDE_INFERRED));
//            
//            System.err.println(Options.QUERY_TIME_EXPANDER + "="
//                    + p.getProperty(Options.QUERY_TIME_EXPANDER));

            // iff LTS.  Other variants use data.dir.
            System.err.println(Options.FILE + "="
                    + p.getProperty(Options.FILE));

            System.err.println(Options.NESTED_SUBQUERY + "="
                    + p.getProperty(Options.NESTED_SUBQUERY));

            System.err.println(Options.CHUNK_CAPACITY + "="
                    + p.getProperty(Options.CHUNK_CAPACITY));
            
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
    
    /**
     * Shows some interesting details about the term2id index.
     * 
     * @param sail
     */
    public static void showLexiconIndexDetails(BigdataSail sail) {
        
        IIndex ndx = sail.getDatabase().getLexiconRelation().getTerm2IdIndex();
        IndexMetadata md = ndx.getIndexMetadata();
        
        System.out.println("Lexicon:");
        System.out.println(md.toString());
        System.out.println(md.getTupleSerializer().toString());
        
    }
    
    /**
     * Shows some interesting details about the primary index for the
     * {@link SPORelation}.
     * 
     * @param sail
     */
    public static void showSPOIndexDetails(final BigdataSail sail) {
        
        final IIndex ndx = sail.getDatabase().getSPORelation().getPrimaryIndex();
        
        final IndexMetadata md = ndx.getIndexMetadata();
        
        System.out.println(md.getName()+":");
        System.out.println(md.toString());
        System.out.println(md.getTupleSerializer().toString());
        
    }

    /**
     * Return various interesting metadata about the KB state.
     * 
     * @todo add some information about nextOffset for the WORM and space waster
     *       for the RW store.
     */
    protected StringBuilder getKBInfo(final AbstractTripleStore tripleStore) {
        
        final StringBuilder sb = new StringBuilder();

//        if (false && tripleStore instanceof LocalTripleStore) {
//
//            // show the disk access details.
//            
//            sb.append(((Journal) tripleStore.getIndexManager())
//                    .getBufferStrategy().getCounters().toString());
//            
//        }
        
//        if(true) return sb;// comment out to get detailed info.
        
        sb.append("class\t"+tripleStore.getClass().getName()+"\n");

        sb.append("indexManager\t"+tripleStore.getIndexManager().getClass().getName()+"\n");
        
        sb.append("exactStatementCount\t"
                + tripleStore.getStatementCount(null/* c */, true/* exact */)
                + "\n");

        sb.append("statementCount\t"
                + tripleStore.getStatementCount(null/* c */, false/* exact */)
                + "\n");

        sb.append("termCount\t" + tripleStore.getTermCount()+"\n");
        
        sb.append("uriCount\t" + tripleStore.getURICount()+"\n");
        
        sb.append("literalCount\t" + tripleStore.getLiteralCount()+"\n");
        
        sb.append("bnodeCount\t" + tripleStore.getBNodeCount()+"\n");
        
//        sb.append(tripleStore.predicateUsage());
        
        return sb;
        
    }
    
    /**
     * The instance under test.
     */
    BigdataSail sail;

    /** Note: iff we need to shutdown the federation ourselves (not for JDS). */
    AbstractFederation fed;

    /**
     * Note: iff JDS since we use this to setup and take down the "embedded" JDS
     * instance.
     */
    JiniServicesHelper jiniServicesHelper;
    
    /**
     * The {@link Result} for this test.
     */
    final Result result = new Result();
//  ret.put("ncommitted",""+ncommitted);
//  ret.put("nfailed",""+nfailed);
//  ret.put("nuncommitted", ""+nuncommitted);
//  ret.put("ntimeout", ""+ntimeout);
//  ret.put("ninterrupted", ""+ninterrupted);
//  ret.put("elapsed(ms)", ""+elapsed);
//  ret.put("operations/sec", ""+(ncommitted * 1000 / elapsed));
////  ret.put("groupCommitCount", ""+groupCommitCount);
////  ret.put("avgCommitGroupSize", ""+(ncommitted/groupCommitCount));
////  ret.put("#overflow", ""+((ResourceManager)dataService.getResourceManager()).getOverflowCount());
//  ret.put("failures", ""+(failures.size()));
//
//  System.err.println(ret.toString(true/*newline*/));
//
//  if(!failures.isEmpty()) {
//      
//      System.err.println("failures:\n"+Arrays.toString(failures.toArray()));
//      
//      fail("There were "+failures.size()+" failed tasks for unexpected causes");
//      
//  }

    static {
        
        // adds "ntriples" as an N-Triples filename extension.
        final RDFFormat NTRIPLES = new RDFFormat("N-Triples", "text/plain",
                Charset.forName("US-ASCII"), Arrays.asList("nt", "ntriples"),
                false, false);

        RDFFormat.register(NTRIPLES);

    }

    public void setUpComparisonTest(Properties properties) throws Exception {

        final DatabaseModel fedType = DatabaseModel.valueOf(properties
                .getProperty(TestOptions.DATABASE_MODEL));

        final String namespace = "kb";

        final long timestamp = ITx.UNISOLATED;

        final File file = File.createTempFile(getClass().getSimpleName(),
                ".test");

        switch (fedType) {

        case LTS:

            /*
             * For the LTS, this is the name of the backing file created and
             * used by the Journal.
             */
            properties.setProperty(Options.FILE, file.toString());

            sail = getSail(namespace, timestamp, properties);

            fed = null;
            
            jiniServicesHelper = null;

            break;

        case LDS: {

            jiniServicesHelper = null;
            
            /*
             * The name of the data directory (LDS).
             */
            properties
                    .setProperty(
                            com.bigdata.service.LocalDataServiceClient.Options.DATA_DIR,
                            file.toString());
            
            /*
             * Delete the temp file before running since LDS will create a
             * directory by that name.
             */
            file.delete();

            fed = new LocalDataServiceClient(properties).connect();

            sail = getSail(fed, namespace, timestamp);

            break;
        
        }
            
        case EDS: {

            jiniServicesHelper = null;
            
            /*
             * The name of the directory in which all of the embedded data services
             * will place their state (EDS).
             */
            properties.setProperty(
                    com.bigdata.service.EmbeddedClient.Options.DATA_DIR, file
                            .toString());
            
            // disable platform statistics collection.
            properties.setProperty(
                    EmbeddedClient.Options.COLLECT_PLATFORM_STATISTICS, "false");
            
            /*
             * Delete the temp file before running since EDS will create a
             * directory by that name.
             */
            file.delete();
            
            fed = new EmbeddedClient(properties).connect();

            sail = getSail(fed, namespace, timestamp);

            break;
            
        }
            
        case JDS:

            /*
             * The file identifies a Jini config directory
             * 
             * FIXME We really need to copy either this into a temporary
             * directory and edit the name of the data directory or we need to
             * use a config file that is specifically for test data and delete
             * everything in the data directory before running the test.
             */

            jiniServicesHelper = new JiniServicesHelper(new String[] { //
                    file.toString() //
                    });

            jiniServicesHelper.start();

            // don't shutdown in finally{} - jiniServicesHelper will do shutdown instead.
            fed = null;

            sail = getSail(jiniServicesHelper.client.connect(), namespace,
                    timestamp);

            break;

        default:
            throw new AssertionError();

        }

        sail.initialize();

        showProperties(getProperties(sail));
        showInterestingProperties(sail.getDatabase());
        getKBInfo(sail.getDatabase());
        showLexiconIndexDetails(sail);
        showSPOIndexDetails(sail);

    }

    public void tearDownComparisonTest() throws Exception {

        final IIndexManager indexManager = sail.getDatabase().getIndexManager();
        
        // shutdown the sail.
        sail.shutDown();

        /*
         * Destroy the database.
         */
        if (jiniServicesHelper != null) {
            
            jiniServicesHelper.destroy();
            
        } else {
            
            indexManager.destroy();
            
        }
            
    }

    /**
     * Typesafe enumeration of the deployment models.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static enum DatabaseModel {
        
        LTS,
        LDS,
        EDS,
        JDS;
        
    }
    
    /**
     * Additional properties understood by this test.
     */
    public static interface TestOptions extends Options {

        /**
         * Any of the constants defined by {@link DatabaseModel}.
         */
        String DATABASE_MODEL = "databaseModel";
        
        /**
         * Optional resource identifying a file containing an ontology.
         */
        String ONTOLOGY = "ontology";

        /**
         * Optional resource identifying a file or directory to be loaded
         * as data.
         */
        String DATA = "data";

        /**
         * Optional resource identifying a file or directory containing SPARQL
         * queries. Queries will be executed after all data has been loaded and
         * the optional closure has been computed.
         */
        String QUERY = "query";

        /*
         * Options for the ConcurrentDataLoader.
         */
        
        /**
         * #of threads to use on each client.
         */
        String CONCURRENT_DATA_LOADER_NTHREADS = "concurrentDataLoader.nthreads";
        /**
         * #of clients to use.
         */
        String CONCURRENT_DATA_LOADER_NCLIENTS = "concurrentDataLoader.nclients";
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
         * Options for query execution.
         * 
         * @todo query language
         * 
         * @todo optional warm up query(s) (either specify the queries or use a
         * boolean; the main point of warm up is to get all of the classes
         * loaded. JVM tuning effects will last for several presentations of any
         * short query. For even modest data sets, presenting a query once means
         * that its data is probably cached by a mixture of the OS and the disk
         * cache.)
         */

        /**
         * The #of trials for each query.
         */
        String NTRIALS = "ntrials";

        /**
         * The #of times that a query is evaluated in parallel with a single
         * trial.
         */
        String NPARALLEL = "nparallel";

    }

    /**
     * Setup and run a test.
     * 
     * @param properties
     */
    public Result doComparisonTest(final Properties properties)
            throws Exception {

        // list of queries to be executed.
        final List<Query> queries = readQueries(properties);

        // used to parse qeries, e.g., SPARQL, etc.
        final QueryParser queryParser = new SPARQLParserFactory().getParser();

        // load the data and optionally compute the closure
        loadData(properties);

        // OS specific hook to drop the file system cache.
        dropFileSystemCache();
        
        // run the queries.
        runQueries(properties, queryParser, queries);

        return result;

    }

    /**
     * @todo only handles linux right now.
     * 
     * @todo move to a test utility class?
     */
    protected void dropFileSystemCache() {
        try {
            if (SystemUtil.isLinux()) {
                /*
                 * See http://linux-mm.org/Drop_Caches
                 * 
                 * sync && echo 3 > /proc/sys/vm/drop_caches
                 */
                Runtime
                        .getRuntime()
                        .exec(new String[] {//
                                "/bin/bash",//
                                        "-c",//
                                        "\"sync && echo 3 > /proc/sys/vm/drop_caches && echo dropped cache\"" });
            } else {
                log.error("Do not know how to drop the file system cache: "
                        + SystemUtil.operatingSystem());
            }
        } catch (IOException ex) {
            log.error("Did drop the file system cache: "
                    + SystemUtil.operatingSystem() + ", ex");
        }
        log.info("Dropped file system cache.");
    }
    
    /**
     * Filter accepts anything that looks like an RDF data file.
     */
    final private FilenameFilter filter = new FilenameFilter() {

        public boolean accept(File dir, String name) {

            if (new File(dir, name).isDirectory()) {

                // visit subdirectories.
                return true;

            }

            // if recognizable as RDF.
            return RDFFormat.forFileName(name) != null;

        }

    };
    
    /**
     * Load the data file(s), updating the closure as necessary.
     * <p>
     * Note: Incremental truth maintenance is performance IFF you are loading
     * using the single threaded loader AND truth maintenance is enabled for the
     * SAIL. Database at once closure is performed iff the axiom model includes
     * at least RDFS and incremental truth maintenance was not performed.
     * <p>
     * Note: This sets the <i>baseURI</i> to the URI form of the name of each
     * file to be loaded.
     * 
     * @throws InterruptedException 
     */
    public void loadData(Properties properties) throws InterruptedException {

        final File dataDir = new File(properties.getProperty(TestOptions.DATA));

        if (!dataDir.exists()) {
            
            throw new RuntimeException("No such file: "+dataDir);
            
        }
        
        /*
         * true iff truth maintenance was performed incrementally during load.
         */
        final boolean didTruthMaintenance;

        /*
         * When true, as the RDF parser to verify the source file.
         * 
         * @todo configure via Properties.
         */
        final boolean verifyRDFSourceData = false;

        /*
         * Properties that are used iff we use the ConcurrentDataLoader.
         */
        final int nthreads = Integer.parseInt(properties.getProperty(
                TestOptions.CONCURRENT_DATA_LOADER_NTHREADS,
                TestOptions.DEFAULT_CONCURRENT_DATA_LOADER_NTHREADS));

        final int nclients = Integer.parseInt(properties.getProperty(
                TestOptions.CONCURRENT_DATA_LOADER_NCLIENTS,
                TestOptions.DEFAULT_CONCURRENT_DATA_LOADER_NCLIENTS));

        final int clientNum = Integer.parseInt(properties.getProperty(
                TestOptions.CONCURRENT_DATA_LOADER_CLIENTNUM,
                TestOptions.DEFAULT_CONCURRENT_DATA_LOADER_CLIENTNUM));

        final int bufferCapacity = Integer.parseInt(properties.getProperty(
                TestOptions.CONCURRENT_DATA_LOADER_BUFFER_CAPACITY,
                TestOptions.DEFAULT_CONCURRENT_DATA_LOADER_BUFFER_CAPACITY));

        final long begin = System.currentTimeMillis();

        if (properties.getProperty(TestOptions.ONTOLOGY) != null) {

            final File ontologyFile = new File(properties
                    .getProperty(TestOptions.ONTOLOGY));

            loadOntology(ontologyFile.getAbsolutePath().toString());
            
        }
        
        /*
         * Load data.
         */
        {
            
            // show the current time when we start.
            System.out.println("Loading data: now="+new Date().toString());
            
            {

                final AbstractTripleStore db = sail.getDatabase();
                
                final long statementCountBefore = db.getStatementCount();

                final long termCountBefore = db.getTermCount();

                final long beginLoad = System.currentTimeMillis();

                if (sail.getDatabase().getIndexManager() instanceof IBigdataFederation) {

                    /*
                     * database at once closure is required with concurrent bulk
                     * load (it does not do incremental truth maintenance).
                     * 
                     * @todo the code could be modified to permit the caller to
                     * specify single threaded data loading with incremental TM
                     * for the federation.
                     */
                    
                    didTruthMaintenance = false;
            
                    loadConcurrent(nthreads, nclients, clientNum,
                            bufferCapacity, dataDir, verifyRDFSourceData);

                } else {

                    // true iff the sail will do incremental truth maintenance.
                    didTruthMaintenance = sail.getTruthMaintenance();

                    loadSingleThreaded(sail.getDatabase(), dataDir);

                }

                /*
                 * Make the changes restart-safe.
                 * 
                 * Note: We already have a commit point if we used the concurent
                 * data loader, but this also writes some info on System.out.
                 * 
                 * Note: this gives us a commit point if closure fails.
                 */
                commit();
                
                final long elapsed = System.currentTimeMillis() - beginLoad;
                
                final long statementCountAfter = db.getStatementCount();
                
                final long termCountAfter = db.getTermCount();

                final long statementsLoaded = statementCountAfter
                        - statementCountBefore;

                final long termsLoaded = termCountAfter - termCountBefore;
                
                final long statementsPerSecond = (long) (statementsLoaded * 1000d / elapsed);

                System.out.println("Loaded data: loadTime(ms)=" + elapsed
                        + ", loadRate(tps)=" + statementsPerSecond
                        + ", toldTriples=" + statementsLoaded + ", #terms="
                        + termsLoaded);

                result.put("toldTriples", "" + statementsLoaded);
                result.put("toldTerms", "" + termsLoaded);
                result.put("loadRate", "" + statementsPerSecond);
                result.put("loadTime", "" + elapsed);
            }
            
        }

        /*
         * If incremental truth maintenance was not performed the axiom model
         * includes at least RDFS then we compute the database-at-once closure.
         */
        final boolean didDatabaseAtOnceClosure = !didTruthMaintenance
                && sail.getDatabase().getAxioms().isRdfSchema();
        
        if (didDatabaseAtOnceClosure) {

            // show the current time when we start.
            System.out.println("Computing closure: now="+new Date().toString());
            
            final long beginClosure = System.currentTimeMillis();
            
//            // show some info on the KB state (before computing closure).
//            System.out.println(getKBInfo());
            
            final InferenceEngine inf = sail.getDatabase().getInferenceEngine();

            // database at once closure.
            final ClosureStats closureStats = inf.computeClosure(null/* focusStore */);
            
            System.out.println("closure: "+closureStats);

            final long elapsed = System.currentTimeMillis() - beginClosure;
            
            result.put("closureTime", "" + elapsed);
            result.put("mutationCount", "" + closureStats.mutationCount);
            
            // make the changes restart-safe
            commit();

        }

        if (didDatabaseAtOnceClosure) {

            /*
             * Report total tps throughput for load+closure. While closure
             * generates solutions very quickly, it produces many non-distinct
             * solutions which reduces the overall throughput.
             */
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            final long ntriples = sail.getDatabase().getStatementCount(false/*exact*/);
            
            final long tps = ((long) (((double) ntriples) / ((double) elapsed) * 1000d));
            
            result.put("totalTriples", "" + ntriples);
            result.put("totalLoadRate", "" + tps);
            result.put("totalLoadTime", "" + elapsed);

            System.err.println("Net: tps=" + tps + ", ntriples=" + ntriples
                    + ", elapsed=" + elapsed + "ms");
            
        }
        
        // show the current time when we start.
        System.out.println("Done: now="+new Date().toString());
        
    }

    /**
     * Loads just the ontology file.
     */
    protected void loadOntology(final String ontology) {

        final long begin = System.currentTimeMillis();
        
        final DataLoader dataLoader = sail.getDatabase().getDataLoader();
        
        // load the ontology.
        try {

            final String filename = ontology;

            System.out.print("Loading ontology: " + ontology + "...");

            final String baseURI = new File(filename).toURI().toString();
            
            final RDFFormat rdfFormat = RDFFormat.forFileName(filename);

            if (rdfFormat == null) {

                throw new RuntimeException("Could not identify RDF format: "
                        + filename);
                
            }
            
            dataLoader.loadData(filename, baseURI, rdfFormat);

            System.out.println("done.");
            
        } catch (Throwable ex) {

            final long elapsed = System.currentTimeMillis() - begin;

            throw new RuntimeException("Exception loading ontology: " + ex
                    + " after " + elapsed + "ms", ex);
            
        }

    }
        
    /**
     * Commits the store and writes some basic information onto {@link System#out}.
     */
    protected void commit() {
        
        // make the changes restart-safe
        sail.getDatabase().commit();

//        if (sail.getDatabase().getIndexManager() instanceof Journal) {
//
//            /*
//             * This reports the #of bytes used on the Journal.
//             */
//            
//            System.out.println("nextOffset: "
//                    + ((Journal) sail.getDatabase().getIndexManager()).getRootBlockView()
//                            .getNextOffset());
//            
//        }

        // show some info on the KB state (#of triples, #of terms).
        System.out.println(getKBInfo(sail.getDatabase()));
        
    }
    
    /**
     * Loads files concurrently into the configured {@link AbstractTripleStore}.
     * 
     * @param nthreads
     *            The #of threads in which files will be loaded.
     * @param bufferCapacity
     *            The capacity of the {@link StatementBuffer} used by each
     *            thread.
     * @param dataDir
     *            The directory from which the filters will be loaded.
     * 
     * @throws InterruptedException
     *             if interrupted.
     */
    protected void loadConcurrent(int nthreads, int nclients, int clientNum,
            int bufferCapacity, File dataDir, boolean verifyRDFSourceData)
            throws InterruptedException {

        System.out.println("Will load files: dataDir=" + dataDir);

        final IBigdataFederation fed = (IBigdataFederation) sail.getDatabase()
                .getIndexManager();

        final RDFFormat fallback = RDFFormat.RDFXML;

        final AbstractTripleStore db = sail.getDatabase();

        final RDFLoadTaskFactory loadTaskFactory = //
        new RDFLoadTaskFactory(db, bufferCapacity, verifyRDFSourceData,
                false/* deleteAfter */, fallback);

        final ConcurrentDataLoader service = new ConcurrentDataLoader(fed,
                nthreads);

        final FileSystemLoader scanner = new FileSystemLoader(service,
                nclients, clientNum);

        try {

            /*
             * Note: Add the counters to be reported to the client's counter
             * set. The added counters will be reported when the client reports its
             * own counters.
             */
            final CounterSet serviceRoot = fed.getServiceCounterSet();

            final String relPath = "Concurrent Data Loader";

            synchronized (serviceRoot) {

                if (serviceRoot.getPath(relPath) == null) {

                    // Create path to CDL counter set.
                    final CounterSet tmp = serviceRoot.makePath(relPath);

                    // Attach CDL counters.
                    tmp.attach(service.getCounters());

                    // Attach task factory counters.
                    tmp.attach(loadTaskFactory.getCounters());

                }
                
            }

            // notify will run tasks.
            loadTaskFactory.notifyStart();

            // read files and run tasks : baseURI will be the filename.
            scanner.process(dataDir, filter, loadTaskFactory);

            // await completion of all tasks.
            service.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        } finally {
            
            // shutdown the load service (a memory leak otherwise!)
            service.shutdown();
            
        }

        // notify did run tasks.
        loadTaskFactory.notifyEnd();

        System.out.println(loadTaskFactory.reportTotals());

    }
    
    /**
     * Loads the file(s).
     * 
     * @param dataDir
     *            The directory containing the files.
     */
    protected void loadSingleThreaded(AbstractTripleStore db, final File dataDir) {

        final DataLoader dataLoader = db.getDataLoader();
        
        // load file/directory of files.
        try {

            dataLoader.loadFiles(dataDir, null/* baseURI */,
                    null/* rdfFormat */, filter);

        } catch (IOException ex) {
            
            throw new RuntimeException("Problem loading file(s): " + ex, ex);
            
        }

    }

    /*
     * Query stuff.
     */

    /**
     * Return a read-historical connection for query.
     */
    protected BigdataSailConnection getQueryConnection() {

        final long timestamp = sail.getDatabase().getIndexManager()
                .getLastCommitTime();
        
        return sail.getReadOnlyConnection(timestamp);
        
    }

    /**
     * Read the queries from file(s), parse them into label and query, and
     * return an ordered collection of those {@link Query}s.
     */
    public List<Query> readQueries(Properties properties) {

        // source file/dir containing queries.
        final File file = new File(properties.getProperty(TestOptions.QUERY));
        
        final List<Query> list = new LinkedList<Query>();
        
        try {

            readQueries(file, list);
            
        } catch (IOException ex) {
            
            throw new RuntimeException("Problem reading queries", ex);
            
        }
        
        return list;

    }

    /**
     * Append queries found in a file or directory to the list of queries.
     * 
     * @param file
     *            A file or directory.
     * @param queries
     *            A mutable list of queries.
     *            
     * @throws IOException 
     */
    protected void readQueries(final File file, final List<Query> queries)
            throws IOException {

        if(!file.exists()) {
            
            throw new FileNotFoundException(file.toString());
            
        }
        
        if(file.isDirectory()) {
            
            final File[] files = file.listFiles();
            
            for( File tmp : files ) {
                
                readQueries(tmp, queries);
                
            }
            
            return;
            
        }

        queries.addAll(parseFile(file));
        
    }
    
    /**
     * Suck in file, parsing into label+query.
     * 
     * <pre>
     *  
     *  # is a comment.
     *  
     *  [X] is a query label.
     *  
     *  everything until the next label is query.
     *  
     * </pre>
     * 
     * @param file
     *            The file.
     * 
     * @return The list of queries found in the file.
     */
    protected List<Query> parseFile(final File file) throws IOException {

        final BufferedReader r = new BufferedReader(new FileReader(file));

        // list of extracted queries.
        final List<Query> list = new LinkedList<Query>();

        try {

            // query label.
            String label = null;

            // query text.
            final StringBuilder sb = new StringBuilder();

            // current line.
            String s;

            while ((s = r.readLine()) != null) {

                final String trimmed = s.trim();
                
                if (trimmed.startsWith("#"))
                    continue;

                if (trimmed.startsWith("[")) {

                    if (label != null) {

                        list.add(newQuery(label,sb));

                        // clear buffer.
                        sb.delete(0/* start */, sb.length()/* end */);

                    }

                    label = s.substring(1, s.indexOf("]"));

                    continue;
                    
                }
                
                sb.append( s );

                sb.append("\n");

            }

            if (label != null) {

                list.add(newQuery(label,sb));

            }

            return list;
            
        } finally {

            r.close();

        }

    }
    
    private Query newQuery(String label, StringBuilder sb) {
        
        if(INFO)
            log.info("Will run query: " + label); // "\n" + sb);
        
        return new Query(label, sb.toString());
        
    }
    
    /**
     * Binds a query and its label together.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class Query {
        
        final private String label;
        final private String query;
        
        public String getLabel() {
            
            return label;
            
        }
        
        public String getQuery() {
            
            return query;
            
        }
        
        public Query(String label, String query) {
       
            this.label = label;
            
            this.query = query;
            
        }
        
    }

    /**
     * Runs the queries.
     * 
     * @param properties
     * @param queryParser
     * @param queries
     * 
     * @throws InterruptedException
     *             if interrupted while running the query tasks or awaiting
     *             their futures.
     */
    public void runQueries(final Properties properties,
            final QueryParser queryParser, final List<Query> queries)
            throws InterruptedException {

        final int nqueries = queries.size();

        final int ntrials = Integer.parseInt(properties
                .getProperty(TestOptions.NTRIALS));

        final int nparallel = Integer.parseInt(properties
                .getProperty(TestOptions.NPARALLEL));
        
        final ResultsOfTrials[] results = new ResultsOfTrials[nqueries];

        System.out.println("#queries=" + nqueries + ", #trials=" + ntrials
                + ", #parallel=" + nparallel);

        /*
         * Run the query trials.
         */
        {
            
            final long begin = System.currentTimeMillis();
            
            int queryCount = 0;

            final Iterator<Query> itr = queries.iterator();

            while (itr.hasNext()) {

                final Query query = itr.next();

                final ResultsOfTrials resultsOfTrials = runTrials(properties,
                        queryParser, query, ntrials, nparallel);

                results[queryCount] = resultsOfTrials;

                /*
                 * Note: Using a common prefix groups these things together.
                 */
                
                final String prefix = "query.";
                
                final String suffix = "." + query.getLabel();
                
                this.result.put(prefix + "elapsed" + suffix, ""
                        + resultsOfTrials.elapsed);
                
                this.result.put(prefix + "nresults" + suffix, ""
                        + resultsOfTrials.nresults);
                
                this.result.put(prefix + "ncompleted" + suffix, ""
                        + resultsOfTrials.ncompleted);
                
                this.result.put(prefix + "consistent" + suffix, ""
                        + resultsOfTrials.consistent);
                
                this.result.put(prefix + "firstCause" + suffix, ""
                        + resultsOfTrials.firstCause);
                
                queryCount++;

            }

            final long queryTime = System.currentTimeMillis() - begin;

            // report the average total query time across the trials.
            result.put("queryTime", "" + queryTime/ntrials);

        }

        /*
         * Write summary.
         * 
         * @todo we should show the condition here for the summary to be useful
         * when examining the console output with more than one condition.
         * However, note that the condition is present in the generated
         * worksheet, so you can just look there.
         */
        {

            System.out.println("\n\nquery\ttime\tresults");
            
            long elapsed = 0L;
            long nresults = 0L;
            
            for (int i = 0; i < nqueries; i++) {

                final ResultsOfTrials resultsOfTrials = results[i];

                System.out.println(resultsOfTrials.toString());
                
                elapsed += resultsOfTrials.elapsed;

                nresults += resultsOfTrials.nresults;
                
            }

            System.out.println("totals\t"+elapsed+"\t"+nresults);

        }
        
    }

    /**
     * Results for a set of trials for some query.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ResultsOfTrials {
       
        /**
         * The query.
         */
        final Query query;

        /**
         * The results for each trial for the query.
         */
        final TrialResult[] trialResults;

        /**
         * Average elapsed query time for all trials regardless of success or
         * failure.
         */
        final long elapsed;
        
        /**
         * #of results for the first trial.
         */
        final long nresults;
        
        /**
         * iff the query results are consistent across the trials.
         */
        final boolean consistent;

        /**
         * #of times the query completed without error.
         */
        final int ncompleted;

        /**
         * First cause for failure for the first trial in which the query
         * failed.
         */
        final Throwable firstCause;
        
        public ResultsOfTrials(final Query query,
                final TrialResult[] trialResults) {

            this.query = query;

            this.trialResults = trialResults;

            final int ntrials = trialResults.length;

            boolean consistent = true;
            
            int ncompleted = 0;
            
            Throwable firstCause = null;

            long elapsed = 0L;
            
            for (int i = 0; i < ntrials; i++) {

                final TrialResult trialResult = trialResults[i];

                elapsed += trialResult.elapsed;
                
                if (trialResult.firstCause == null) {

                    ncompleted++;

                    if (consistent && i > 0
                            && trialResult.nresults != trialResults[0].nresults) {

                        consistent = false;

                    }

                } else {

                    if (firstCause == null) {

                        firstCause = trialResult.firstCause;

                    }

                }

            }

            this.elapsed = elapsed / ntrials;

            this.nresults = trialResults[0].nresults;
            
            this.consistent = consistent;

            this.ncompleted = ncompleted;

            this.firstCause = firstCause;
            
        }

        public String toString() {
            
            final int ntrials = trialResults.length;

            final StringBuilder sb = new StringBuilder();
            
            sb.append(trialResults[0].query.getLabel());
            
            sb.append('\t');
            
            sb.append(Long.toString(elapsed));
            
            sb.append('\t');

            if(consistent) {
            
                sb.append(Long.toString(nresults));
                
            } else {
                
                final long[] a = new long[ntrials];

                for (int j = 0; j < ntrials; j++) {

                    a[j] = trialResults[j].nresults;

                }
                
                sb.append("*** INCONSISTENT " + Arrays.toString(a));
                
            }

            if (firstCause != null) {
                
                sb.append("*** ERRORS: firstCause=" + firstCause
                        + ", ncomplete=" + ncompleted + ", ntrials=" + ntrials);
            
            }
            
            return sb.toString();

        }
        
    }
    
    /**
     * Run <i>ntrials</i> of the query.
     * 
     * @param properties
     * @param queryParser
     * @param query
     * @param ntrials
     * @param nparallel
     * 
     * @return An array containing the results for each trial.
     * 
     * @throws InterruptedException
     */
    public ResultsOfTrials runTrials(final Properties properties,
            final QueryParser queryParser, final Query query,
            final int ntrials, final int nparallel) throws InterruptedException {

        System.out.println("\n\n----  " + query.getLabel() + " ----\n");

        final TrialResult[] results = new TrialResult[ntrials];

        for (int trialCount = 0; trialCount < ntrials; trialCount++) {

            final TrialResult trialResult = runTrial(properties, queryParser,
                    query, nparallel);

            results[trialCount] = trialResult;

            if (trialCount > 0 && trialResult.nresults != results[0].nresults) {

                log.warn("INCONSISTENT: trial#" + trialCount + ", query="
                        + query.getLabel());

            }

        }

        return new ResultsOfTrials(query, results);
        
    }

    /**
     * Run a single trial of the query.
     * 
     * @param properties
     * @param queryParser
     * @param query
     *            The query.
     * @param nparallel
     *            The #of parallel presentations of the query.
     * 
     * @return The results of the trial.
     * 
     * @throws InterruptedException
     */
    public TrialResult runTrial(final Properties properties,
            final QueryParser queryParser, final Query query,
            final int nparallel) throws InterruptedException {

        final ExecutorService service = sail.getDatabase().getIndexManager()
                .getExecutorService();

        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(
                nparallel);

        for (int i = 0; i < nparallel; i++) {

            tasks.add(new QueryTask(queryParser, query));

        }

        final long begin = System.currentTimeMillis();
        
        final List<Future<Long>> futures = service.invokeAll(tasks);

        final long elapsed = System.currentTimeMillis() - begin;
        
        final Iterator<Future<Long>> itr = futures.iterator();

        int nfailed = 0;

        final long[] nresults = new long[nparallel];

        final Throwable[] firstCauses = new Throwable[nparallel];

        int i = 0;

        while (itr.hasNext()) {

            final Future<Long> f = itr.next();
            
            try {
                
                nresults[i] = f.get();
                
            } catch (InterruptedException e) {

                // interrupted while awaiting future.
                throw e;
                
            } catch (ExecutionException e) {
                
                nfailed++;

                // indicates execution error.
                nresults[i] = -1L;

                // note the cause.
                firstCauses[i] = e;
                
                log.warn("Problem executing query[" + i + "]", e);
            
            }
            
            i++;
            
        }

        final TrialResult trialResult = new TrialResult(query, elapsed,
                nresults, firstCauses);

        System.out.println(trialResult.toString());
        
        return trialResult;
        
    }

    /**
     * Results for a single trial (n presentations of a query in parallel).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class TrialResult {

        /** The query. */
        public final Query query;
        
        /** elapsed time for the trial. */
        public final long elapsed;
        
        /** #of results per query presentation in the trial. */
        public final long[] results;

        /**
         * The first cause reported for each query presentation in the trial and
         * <code>null</code> iff the query completed without error.
         */
        public final Throwable[] firstCauses;
        
        /** average #of results across the trials. */
        public final long nresults;

        /** <code>true</code> unless the #of results is not consistent. */
        public final boolean consistent;
        
        /**
         * The first cause reported for the first query presentation that did
         * not complete normally. While the choice here is somewhat arbitrary,
         * this serves as a signal to the next layer of aggregation that all is
         * not well.
         */
        public final Throwable firstCause;

        /**
         * @param query
         *            The query.
         * @param elapsed
         *            Elapsed time.
         * @param results
         *            #of results for each parallel presentation of the same
         *            query.
         * @param firstCauses
         *            An array whose elements are the exceptions thrown for each
         *            query presentation or a <code>null</code> if a given
         *            query presentation completed without error.
         */
        public TrialResult(final Query query, final long elapsed,
                final long[] results, final Throwable[] firstCauses) {

            this.query = query;
            
            this.elapsed = elapsed;

            this.results = results;

            this.firstCauses = firstCauses;

            assert results.length == firstCauses.length;
            
            long n = results[0];

            boolean consistent = true;

            Throwable firstCause = null;
            
            for (int i = 1; i < results.length; i++) {

                if (firstCauses[i] != null) {

                    if (firstCause == null) {

                        firstCause = firstCauses[i];

                    }

                }
                
                if (results[i] != n) {

                    consistent = false;

                }

            }

            this.nresults = n;

            this.consistent = consistent;

            this.firstCause = firstCause;
            
        }

        /**
         * Summary of the query trial.
         */
        public String toString() {

            return query.getLabel()
                    + "\t"
                    + elapsed
                    + "\t"
                    + (firstCause == null ? (consistent ? "" + nresults
                            : "INCONSISTENT " + Arrays.toString(results))
                            : firstCause.getLocalizedMessage());
            
        }
        
    }
    
    /**
     * Task executes a query.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class QueryTask implements Callable<Long> {
        
        final QueryParser queryParser;
        
        final Query query;
        
        public QueryTask(final QueryParser queryParser, final Query query) {
        
            this.queryParser = queryParser;
            
            this.query = query;
            
        }

        /**
         * Run the query.
         * 
         * @return The #of results for that query.
         * 
         * @throws Exception
         *             whatever the query threw when it ran.
         */
        public Long call() throws Exception {

            return issueQuery(queryParser, query);
            
        }
        
    }
    
    /**
     * Runs a query.
     * 
     * @param queryParser
     *            Knows how to handle queries for a specific query language,
     *            e.g., SPARQL.
     * 
     * @param theQuery
     *            The {@link Query}.
     * 
     * @return The #of results for the query.
     * 
     * @throws Exception
     *             whatever the query throws when it runs.
     */
    protected long issueQuery(final QueryParser queryParser,
            final Query theQuery) throws Exception {

//        /*
//         * Note: These are the properties that are currently configured for the
//         * tripleStore.
//         */
//        final Properties properties = db.getProperties();
        
        /*
         * When <code>true</code> inferences will be included in the results
         * for high-level query.
         */
        final boolean includeInferred = true;
        
        final String baseURI = null; // @todo what baseURI for queries?
        
            final String queryString = theQuery.getQuery();

        if (DEBUG)
            log.debug("query: " + queryString);

        final ParsedQuery query = queryParser.parseQuery(queryString, baseURI);

        /*
         * Create a data set consisting of the contexts to be queried.
         * 
         * Note: a [null] DataSet will cause context to be ignored when the
         * query is processed.
         */
        final DatasetImpl dataSet = null; // new DatasetImpl();

        final BindingSet bindingSet = new QueryBindingSet();

        final BigdataSailConnection conn = getQueryConnection();

        // Note: Will close the [conn] for all outcomes.
        final MyQueryResult result = new MyQueryResult(conn, conn.evaluate(
                query.getTupleExpr(), dataSet, bindingSet, includeInferred));

        long n = 0;

        while (result.next()) {

            n++;

        }

        return n;

    }

    /**
     * Counts the #of results from the query and closes the
     * {@link SailConnection}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MyQueryResult {

        private BigdataSailConnection conn;

        private CloseableIteration<? extends BindingSet, QueryEvaluationException> itr;

        private long nsolutions = 0;

        public MyQueryResult(
                BigdataSailConnection conn,
                CloseableIteration<? extends BindingSet, QueryEvaluationException> itr) {

            this.conn = conn;

            this.itr = itr;

        }

        protected void finalized() throws Throwable {

            close();

        }

        private void close() {

            if (itr != null) {

                try {
                    
                    itr.close();
                    
                    itr = null;
                    
                } catch (QueryEvaluationException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }

            if (false) {
                /*
                 * @todo disable - this shows the effective bloom filter error
                 * rate to date (or at least since the index object was last
                 * read from the store).
                 * 
                 * Note: The repository is re-opened before each set of
                 * presentations of a given query, so the false positive error
                 * rate climbs only during a given set of presentations of the
                 * same query.
                 * 
                 * Note: This does not report anything for the scale-out
                 * federations since the joins are performed using the index
                 * partitions not the scale-out indices.
                 */
                final IIndex ndx = conn.getTripleStore().getSPORelation()
                        .getPrimaryIndex();

                if (ndx instanceof ILocalBTreeView) {

                    /*
                     * Note: For a fused view, the false positives are reported
                     * to the mutable btree.
                     */
                    final BTree btree = ((ILocalBTreeView) ndx)
                            .getMutableBTree();

                    final BloomFilter filter = btree.getBloomFilter();
                    
                    if (filter != null)
                        System.err.println(filter.counters
                                .getBloomFilterPerformance());

                }

            }
            
            /*
             * FIXME test on isReadOnly() is a workaround for the sail which
             * does not hand us a distinct BigdataSailConnection for each
             * read-historical or each read-committed view.
             */
            if (conn != null && !conn.getTripleStore().isReadOnly()) {

                try {

                    conn.close();

                } catch (SailException e) {
                    
                    throw new RuntimeException(e);
                    
                } finally {
                    
                    conn = null;
                    
                }

            }
            
        }
        
        public long getNum() {

            return nsolutions;

        }

        public boolean next() {

            if (itr == null)
                throw new IllegalStateException();
            
            if (conn == null)
                throw new IllegalStateException();
            
            try {

                if (!itr.hasNext()) {

                    close();

                    return false;

                }

                final BindingSet bindingSet = itr.next();
                
                // dumps the binding set - uncomment to verify the solutions.
//                System.err.println(bindingSet.toString());

                nsolutions++;

                return true;

            } catch (QueryEvaluationException e) {

                close();

                throw new RuntimeException(e);

            }

        }

    }

    /**
     * Return the {@link Properties} that may be used to configure a new
     * {@link AbstractTripleStore} instance. The {@link AbstractTripleStore}
     * will remember the properties with which it was created and use those
     * values each time it is re-opened. The properties are stored in the global
     * row store for the backing {@link IIndexManager}.
     */
    static protected Properties getDefaultProperties() {

        // pick up system properties as defaults.
//        final Properties properties = new Properties(System.getProperties());
        // do not pick up system properties as defaults.
        final Properties properties = new Properties();

        properties.setProperty(TestOptions.NTRIALS, "10");
        properties.setProperty(TestOptions.NPARALLEL, "1");
        
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
//        properties.setProperty(Options.AXIOMS_CLASS,RdfsAxioms.class.getName());
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
         * Turn on bloom filter for the SPO index (good up to ~2M index entries
         * for scale-up -or- for any size index for scale-out).
         */
        properties.setProperty(Options.BLOOM_FILTER, "true");

        /*
         * Turn off statement identifiers.
         */
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

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
        /*
         * Choice of the join algorithm.
         * 
         * false is pipeline.
         * 
         * true is nested, which is also the default right now.
         */
//        properties.setProperty(Options.NESTED_SUBQUERY, "false");

        //        properties.setProperty(Options.BRANCHING_FACTOR,"64");// default is 32
        
        {
         
            /*
             * Note: The JDS and JDSTest properties are set in the individual config
             * files!!! The settings here only effect the LDS and EDS
             * configurations.
             */

//            /*
//             * Don't collect statistics from the OS for either the
//             * IBigdataClient or the DataService.
//             */
//            properties
//                    .setProperty(
//                            IBigdataClient.Options.COLLECT_PLATFORM_STATISTICS,
//                            "false");
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
            properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");

            // Disable overflow of the live journal.
            properties.setProperty(OverflowManager.Options.OVERFLOW_ENABLED,"false");

            // Disable index partition moves.
            properties.setProperty(OverflowManager.Options.MAXIMUM_MOVES_PER_TARGET,"0");
            
        }
        
        /*
         * May be used to turn off query-time expansion of entailments such as
         * (x rdf:type rdfs:Resource) and owl:sameAs even through those
         * entailments were not materialized during forward closure.
         */
//        properties.setProperty(Options.QUERY_TIME_EXPANDER, "false");

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
     * Runs a single instance of the test as configured in the code.
     * 
     * @see ExperimentDriver, which parameterizes the use of this stress test.
     *      That information should be used to limit the #of transactions
     *      allowed to start at one time on the server and should guide a search
     *      for thinning down resource consumption, e.g., memory usage by
     *      btrees, the node serializer, etc.
     * 
     * @see GenerateExperiment, which may be used to generate a set of
     *      conditions to be run by the {@link ExperimentDriver}.
     */
    public static void main(String[] args) throws Exception {

        final Properties properties = getDefaultProperties();

        properties.setProperty(TestOptions.DATABASE_MODEL, DatabaseModel.LTS
                .toString());

        /*
         * Note: For simplicity, this is a copy of LUBM U1 with the ontology
         * file included in the directory.
         */
        properties.setProperty(TestOptions.DATA, "data//U1");

        // The file containing the queries.
        properties.setProperty(TestOptions.QUERY, "data/lubm.query.sparql");
        
        final IComparisonTest test = new LoadClosureAndQueryTest();
        
        test.setUpComparisonTest(properties);
        
        try {

            test.doComparisonTest(properties);
        
        } finally {

            try {
                
                test.tearDownComparisonTest();
                
            } catch(Throwable t) {

                log.warn("Tear down problem: "+t, t);
                
            }
            
        }

    }

    /**
     * Experiment generation utility class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class GenerateExperiment extends ExperimentDriver {

        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(String[] args) throws Exception {

            // this is the test to be run.
            final String className = LoadClosureAndQueryTest.class.getName();

            /*
             * Set defaults for each condition.
             */

            final Map<String, String> defaultProperties = new HashMap<String, String>();

            // disable platform statistics collection.
            defaultProperties
                    .put(IBigdataClient.Options.COLLECT_PLATFORM_STATISTICS,
                            "false");

            defaultProperties.putAll(PropertyUtil
                    .flatten(getDefaultProperties()));

            /*
             * Note: For simplicity, this is a copy of LUBM U1 with the ontology
             * file included in the directory.
             */
            defaultProperties.put(TestOptions.DATA, "data//U1");

            // The file containing the queries.
            defaultProperties.put(TestOptions.QUERY, "data/lubm.query.sparql");

            List<Condition> conditions = new ArrayList<Condition>();

            conditions.add(new Condition(defaultProperties));

            /*
             * Runs each database model.
             */
            conditions = apply(conditions, new NV[][] {
                    new NV[] { new NV(TestOptions.DATABASE_MODEL,
                            DatabaseModel.LTS.toString()) },
                    new NV[] { new NV(TestOptions.DATABASE_MODEL,
                            DatabaseModel.LDS.toString()) },
                    new NV[] { new NV(TestOptions.DATABASE_MODEL,
                            DatabaseModel.EDS.toString()) }
            /*
             * @todo EDS with overflow and splits (verify index partition
             * handling).
             * 
             * @todo JDS (verify serialization, proxy handling).
             */
            });
            
            /*
             * Do condition for each database model in which we use nested joins
             * and in which we use pipeline joins. Note that the choice here
             * applies to be closure (if closure is performed) and to query.
             */
            conditions = apply(conditions, new NV[][] {
                    new NV[] { new NV(Options.NESTED_SUBQUERY, "true") },
                    new NV[] { new NV(Options.NESTED_SUBQUERY, "false") },
            //
            });
            
            final Experiment exp = new Experiment(className, defaultProperties,
                    conditions);

            /* Note: copy the output into a file and then you can run it later. */
            System.err.println(exp.toXML());

        }

    }

}
