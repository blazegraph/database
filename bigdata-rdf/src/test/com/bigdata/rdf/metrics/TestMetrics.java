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
 * Created on Jun 21, 2006
 */

package com.bigdata.rdf.metrics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.CognitiveWeb.util.PropertyUtil;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.journal.ResourceManager;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.store.DataLoader;

/**
 * Test harness for loading randomly generated files into a repository.
 * 
 * @todo Modidy to permit running as load N then close vs load+close for each of
 *       N?
 * 
 * @todo Support concurrent query against the repository. Concurrent query
 *       should begin after some number of files or triples have been loaded.
 *       The query concurrency should be a parameter so that we can test both
 *       platforms that support a single reader or write, or one or more readers
 *       concurrent with a writer. (Some platforms also support concurrent
 *       writers and the test harness should be extended to test that feature as
 *       well.) Testing concurrent operations requires stacked sails for some
 *       platforms in order to enforce serialization of operations for platforms
 *       that do not support concurrent operations.
 * 
 * @todo Make it easy and safe to test concurrent query with a data store once
 *       data load operations have been completed. Among other things, in this
 *       mode we do NOT clear the repository or delete the database.
 * 
 * @todo Make sure that the query generator and the query parser share the same
 *       assumptions for encoding of Unicode.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMetrics extends AbstractMetricsTestCase {

    /**
     * Additional properties defined for the metrics test harness.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class RuntimeOptions //extends AbstractRepositoryTestCase.RuntimeOptions
    {
       
        /**
         * The pathname of the directory containing the documents to be loaded
         * (required). The contents of this directory will be recursively
         * scanned. Any files found will be loaded into the repository. The
         * files must be RDF/XML.
         */
        public static final String DOCUMENTS_DIRECTORY = "documents.directory";

        /**
         * The optional filename of an RDF/XML file containing an ontology to
         * load as before loading the files in the {@link #DOCUMENTS_DIRECTORY}.
         */
        public static final String DOCUMENTS_ONTOLOGY = "documents.ontology";
        
        /**
         * Optional property specifying the maximum #of files to be loaded. When
         * zero (0) all files will be loaded (default 0).
         */
        public static final String MAX_FILES = "maxFiles";
        
        public static final String DEFAULT_MAX_FILES = "0";

        /**
         * The name of the output file (defaults to <code>TestMetrics-metrics.csv</code>).
         */
        public static final String METRICS_FILE = "metricsFile";
        
        public static final String DEFAULT_METRICS_FILE = TestMetrics.class.getSimpleName()+"-metrics.csv";
        
    }
    
//    final File statisticsFile;
    final File metricsFile;
    final File documentOntology;
    final File documentDir;
    final int maxFiles;
    final Writer metricsWriter;
    
    long elapsedLoadTime = 0l;
    
    public TestMetrics( String name ) throws IOException
    {
        
        super( name );
        
        if( name == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        System.err.println( "\n***\n*** "+name+"\n***\n");       
        
        /*
         * Set up formatting for integers. 
         */
        
        nf = NumberFormat.getIntegerInstance();
        
        // grouping is disabled since we write comma-separated-value files.
        nf.setGroupingUsed(false);
        
        fpf = NumberFormat.getNumberInstance();
        
        fpf.setGroupingUsed(false);
        
        fpf.setMaximumFractionDigits(3);
        
//        System.err.println("12.1 : "+nf.format(12.1f));
//        System.err.println("12.5 : "+nf.format(12.5f));
//        System.err.println("12.6 : "+nf.format(12.6f));
//        System.err.println("112.5 : "+nf.format(112.5f));
//        /*
//         * Example of formatting for a units/sec value. Note the cast to
//         * floating point before dividing the units by the milliseconds and
//         * _then_ multiple through by 1000 to get units/sec.
//         */
//        System.err.println("400/855*1000 : "+nf.format(400./855*1000));
//        if(true) System.exit(1);

//      System.err.println("12.1 : "+fpf.format(12.1f));
//      System.err.println("12.5 : "+fpf.format(12.5f));
//      System.err.println("12.6 : "+fpf.format(12.6f));
//      System.err.println("112.5 : "+fpf.format(112.5f));
//      /*
//       * Example of formatting for a units/sec value. Note the cast to
//       * floating point before dividing the units by the milliseconds and
//       * _then_ multiple through by 1000 to get units/sec.
//       */
//      System.err.println("400/855*1000 : "+fpf.format(400./855*1000));
//      if(true) System.exit(1);

        /*
         * Extract and process various properties.
         */
        
        Properties properties = getProperties();

        /*
         * Where to load the ontology (optional).
         */
        {
         
            final String documentOntologyName = properties
                    .getProperty(RuntimeOptions.DOCUMENTS_ONTOLOGY);

            if (documentOntologyName != null) {

                log.info(RuntimeOptions.DOCUMENTS_ONTOLOGY+"="+documentOntologyName);

                documentOntology = new File(documentOntologyName);

                if (!documentOntology.isFile()) {

                    throw new RuntimeException("Ontology file not found: "
                            + documentOntology);

                }

            } else {

                documentOntology = null;

            }
            
        }

        /*
         * Where to load the documents files.
         */
        {

            final String documentDirName = properties
                    .getProperty(RuntimeOptions.DOCUMENTS_DIRECTORY);

            if (documentDirName == null) {

                throw new RuntimeException(RuntimeOptions.DOCUMENTS_DIRECTORY
                        + " : not defined.");

            }

            log.info(RuntimeOptions.DOCUMENTS_DIRECTORY+"="+documentDirName);

            documentDir = new File(documentDirName);

            if (!documentDir.isDirectory()) {

                throw new RuntimeException("Document directory not found: "
                        + documentDir);

            }

        }

        /*
         * Open the metrics file and write the column headers.
         * 
         * Note: If the file already exists then it is deleted 1st.
         */
        {

            final String filename = properties.getProperty(
                    RuntimeOptions.METRICS_FILE,
                    RuntimeOptions.DEFAULT_METRICS_FILE);

            log.info(RuntimeOptions.METRICS_FILE+"="+filename);
            
            metricsFile = new File( filename );
    
            deleteFile( ""+metricsFile );
    
            metricsWriter = new BufferedWriter( new FileWriter(metricsFile));

        }

        {
            
            maxFiles = Integer
                    .parseInt(properties.getProperty(RuntimeOptions.MAX_FILES,
                            RuntimeOptions.DEFAULT_MAX_FILES));

            log.info(RuntimeOptions.MAX_FILES+"="+maxFiles);
            
            if(maxFiles<0) {
                
                throw new RuntimeException(RuntimeOptions.MAX_FILES+" must be non-negative");
                
            }
            
        }
        
    }

    /**
     * <p>
     * Log metrics from loading a single document into the repository and track
     * cumulative metrics.
     * </p>
     * <p>
     * These are the column definitions for the metrics file. Each line
     * corresponds to the load of a single document.
     * <dl>
     * <dt>filesLoaded </dt>
     * <dd> #of files loaded so far 1, 2, .....</dd>
     * <dt>elapsedTime</dt>
     * <dd>The elapsed clock time since the start of the test(ms).</dd>
     * <dt>transactionsPerSec</dt>
     * <dd>The average #of transactions per second (filesLaoded / elapsedTime).</dd>
     * <dt>loadTime</dt>
     * <dd>Time to load the triples into the repository and compute any
     * entailments maintained by the SAIL.</dd>
     * <dt>commitTime</dt>
     * <dd>Time to commit the transaction (ms).</dd>
     * <dt>transactionTime </dt>
     * <dd>elapsed time to load this file (ms). This only counts the spent
     * actually loading the file. It does NOT count the time required to make
     * the data stable on disk (the transaction commit).</dd>
     * <dt>toldTriples</dt>
     * <dd>#of told triples in the loaded file (no entailments).</dd>
     * <dt>toldTriplesPerSec1</dt>
     * <dd>The #of told triples loaded per second for this file
     * <em>excluding</em> commit processing.</dd>
     * <dt>toldTriplesPerSec2</dt>
     * <dd>The #of told triples loaded per second for this file
     * <em>including</em> commit processing.</dd>
     * <dt>totalLoadTime</dt>
     * <dd>Cumulative running total of the loadTime column (ms).</dd>
     * <dt>totalCommitTime</dt>
     * <dd>Cumulative running total of the commitTime column (ms).</dd>
     * <dt>totalTransactionTime</dt>
     * <dd>Cumulative running total of the transactionTime column (ms).</dd>
     * <dt>avgLoadTime</dt>
     * <dd>Running average of the loadTime column (ms).</dd>
     * <dt>avgCommitTime</dt>
     * <dd>Running average of the commitTime column (ms).</dd>
     * <dt>avgTransactionTime</dt>
     * <dd>Running average of the transactionTime column (ms).</dd>
     * <dt>avgToldTriplesPerSec1</dt>
     * <dd>The average #of told triples loaded per second for all files
     * <em>excluding</em> commit processing ( totalToldTriples /
     * totalLoadTime).</dd>
     * <dt>avgToldTriplesPerSec2</dt>
     * <dd>The average #of told triples loaded per second for all files
     * <em>including</em> commit processing ( totalToldTriples /
     * totalTransactionTime).</dd>
     * <dt>totalToldTriples</dt>
     * <dd>Cumulative running total of the toldTriples column. There may be
     * fewer triples in the repository since duplicate triples are not stored.
     * There may be more triples in the repository since entailments may be
     * stored.</dd>
     * <dt>triplesInStore</dt>
     * <dd>The total #of triples in the store (axioms, inferences, and told
     * triples) or zero if not using the GOM SAIL.</dd>
     * <dt>inferencesInStore</dt>
     * <dd>The total #of inferences in the store or zero if not using the GOM
     * SAIL.</dd>
     * <dt>proofsInStore</dt>
     * <dd>The total #of proofs in the store or zero if not using the GOM SAIL.</dd>
     * <dt>urisInStore</dt>
     * <dd>The total #of uris in the store or zero if not using the GOM SAIL.</dd>
     * <dt>bnodesInStore</dt>
     * <dd>The total #of blank nodes in the store or zero if not using the GOM
     * SAIL.</dd>
     * <dt>literalsInStore</dt>
     * <dd>The total #of literals in the store or zero if not using the GOM
     * SAIL.</dd>
     * <dt>totalMemory</dt>
     * <dd>The total VM memory (in Megabytes) as reported by
     * {@link Runtime#totalMemory()}.</dd>
     * <dt>error</dt>
     * <dd>This column is <code>Ok</code> if there was no error. Otherwise
     * this column will contain the error message and the <i>toldTriplesLoaded</i>
     * will be set to zero.</dd>
     * <dt>filename </dt>
     * <dd>The name of the loaded file.</dd>
     * </dl>
     * </p>
     * <p>
     * Additional metrics are available after the run iff you are using one of
     * the GOM SAILs. Those metrics will be found in the <code>-stats.csv</code>
     * file in the same directory as the metrics log. Those data include:
     * </p>
     * 
     * <pre>
     *   triplesInStore    - #of triples in the repository
     *   inferencesInStore - #of inferences in the repository
     *   proofsInStore     - #of proofs in the repository.
     *   urisInStore       - #of URIs in the repository.
     *   bnodesInStore     - #of blank nodes in the repository.
     *   literalsInStore   - #of literals in the repository.
     *   sizeOnDisk        - #of bytes, MB, GB on disk for the repository.
     * </pre>
     * 
     * @param trial
     *            The metrics associated with a single trial.
     * 
     * @throws IOException
     *             If there is a problem writing on the metrics log.
     */
    protected void logMetrics(Trial t) throws IOException {
        
        filesLoaded++;
        final long elapsedTime = System.currentTimeMillis() - beginRunTime;
        totalLoadTime += t.loadTime;
        totalCommitTime += t.commitTime;
        totalTransactionTime += t.transactionTime;
        totalToldTriples += t.toldTriples;
        totalInferenceCount += t.inferencesAdded;
        totalProofCount += t.proofsAdded;
        
        // filesLoaded
        metricsWriter.write(""+filesLoaded+", ");
        // elapsedTime
        metricsWriter.write(""+elapsedTime+", ");
        // transactionsPerSecond
        metricsWriter.write(""+fpf.format(getUnitsPerSecond(filesLoaded,
                elapsedTime))+", ");
//        metricsWriter.write(""
//                + ((filesLoaded == 0 || totalElapsedRunTime == 0) ? 0
//                        : (filesLoaded / ((totalElapsedRunTime < 1000 ? 1000
//                                : totalElapsedRunTime) / 1000))) + ", ");
        // loadTime (ms)
        metricsWriter.write(""+t.loadTime+", ");
        // commitTime (ms)
        metricsWriter.write(""+t.commitTime+", ");
        // transactionTime (ms)
        metricsWriter.write(""+t.transactionTime+", ");
        // toldTriples
        metricsWriter.write(""+t.toldTriples+", ");
        // toldTriplesPerSec1 (excluding commit processing)
        metricsWriter.write((t.loadTime==0?"0":nf.format(getUnitsPerSecond(t.toldTriples,
                t.loadTime)))+", ");
        // toldTriplesPerSec2 (including commit processing)
        metricsWriter.write((t.transactionTime==0?"0":nf.format(getUnitsPerSecond(t.toldTriples,
                t.transactionTime)))+", ");
//        metricsWriter.write(""
//                + ((toldTriplesLoaded == 0 || elapsedLoadTime == 0) ? 0
//                        : (toldTriplesLoaded / ((elapsedLoadTime < 1000 ? 1000
//                                : elapsedLoadTime) / 1000))) + ", ");
        // totalLoadTime (ms)
        metricsWriter.write(""+totalLoadTime+", ");
        // totalCommitTime (ms)
        metricsWriter.write(""+totalCommitTime+", ");
        // totalTransactionTime (ms)
        metricsWriter.write(""+totalTransactionTime+", ");
        // avgLoadTime (ms)
        metricsWriter.write(""+nf.format(totalLoadTime/filesLoaded)+", ");
        // avgCommitTime (ms)
        metricsWriter.write(""+nf.format(totalCommitTime/filesLoaded)+", ");
        // avgTransactionTime (ms)
        metricsWriter.write(""+nf.format(totalTransactionTime/filesLoaded)+", ");
        // avgTriplesPerSec1 (excluding commit processing)
        metricsWriter.write(nf.format(getUnitsPerSecond(totalToldTriples,
                totalLoadTime))+", ");
        // avgTriplesPerSec2 (including commit processing)
        metricsWriter.write(nf.format(getUnitsPerSecond(totalToldTriples,
                totalTransactionTime))+", ");
//        metricsWriter.write(""
//                + ((totalToldTriples == 0 || totalElapsedRunTime == 0) ? 0
//                        : (totalToldTriples / ((totalElapsedRunTime < 1000 ? 1000
//                                : totalElapsedRunTime) / 1000))) + ", ");
        
        // GOM SAIL Specific columns.
        metricsWriter.write(""+t.statementsAdded+", " );
        metricsWriter.write(""+t.inferencesAdded+", " );
        metricsWriter.write(""+t.proofsAdded+", " );
        metricsWriter.write(""+t.urisAdded+", " );
        metricsWriter.write(""+t.bnodesAdded+", " );
        metricsWriter.write(""+t.literalsAdded+", " );
        metricsWriter.write(""+totalToldTriples+", "); // available for all stores.
        metricsWriter.write(""+t.statementCount1+", ");
        metricsWriter.write(""+totalInferenceCount+", ");
        metricsWriter.write(""+totalProofCount+", ");
        metricsWriter.write(""+t.uriCount1+", ");
        metricsWriter.write(""+t.bnodeCount1+", ");
        metricsWriter.write(""+t.literalCount1+", ");

        // total VM memory (in MB).
        metricsWriter.write(""+Runtime.getRuntime().totalMemory()/Bytes.megabyte+", ");
        // error
        metricsWriter.write(""+(t.error == null?"Ok":t.error.getMessage())+", ");
        // filename
        metricsWriter.write(""+t.file+"\n");

        StringBuilder sb = new StringBuilder();

        sb.append("#loaded=" + filesLoaded
                +", elapsed="+elapsedLoadTime
//                + ", told="+t.toldTriples
                + ", told="+ totalToldTriples
                + ", inf="+totalInferenceCount
                + ", proofs="+totalProofCount
//                + ")"
                + " in "
                + t.transactionTime
                + "(ms): average stmts/sec="+ fpf.format(getUnitsPerSecond((totalToldTriples+totalInferenceCount),elapsedLoadTime))
//                + ", file=" + t.file
                );
    
        System.err.println(sb.toString());
        
        if (filesLoaded % 1000 == 0) {

            /*
             * Note: this will slow things down a bit since it will sync the
             * disk but it makes the data safe and you can tail the log to see
             * what is happening.
             */

            metricsWriter.flush();

            usage();
        
        }
        
    }

    /**
     * Invoked to write out the defined configuration properties and metrics
     * column headers after the repository has been initialized.
     * 
     * @throws IOException
     */
    protected void writeMetricsLogHeaders() throws IOException {

        /*
         * Write out the repositoryClass and all defined properties.
         */
//        metricsWriter.write("repositoryClass, "+m_repo.getClass().getName()+"\n");
        metricsWriter.write("host, "+InetAddress.getLocalHost().getHostName()+"\n");
        if(true) {
            Map props = new TreeMap(PropertyUtil.flatten(getProperties()));
            Iterator itr = props.entrySet().iterator();
            while( itr.hasNext() ) {
                Map.Entry entry = (Map.Entry) itr.next();
                String pname = (String)entry.getKey();
                String pvalue = (String) entry.getValue();
                if( pname.equals("repositoryClass")) continue; // already written.
                if( pname.startsWith("line.")) continue;
                if( pname.startsWith("path.")) continue;
                if( pname.startsWith("os.")) continue;
                if( pname.startsWith("java.") && !(
                        pname.equals("java.vm.vendor") ||
                        pname.equals("java.vm.version") ||
                        pname.equals("java.vm.name")
                        )) continue;
                if( pname.startsWith("sun.")) continue;
                if( pname.startsWith("file.")) continue;
                if( pname.startsWith("user.")) continue;
                if( pname.startsWith("maven.")) continue;
                if( pname.startsWith("awt.")) continue;
                if( pname.startsWith("junit.")) continue;
                if( pname.startsWith("cactus.")) continue;
                if( pname.startsWith("tag")) continue; // tag1, tag2, etc. (maven).
                metricsWriter.write(pname+", \""+pvalue+"\"\n");
            }
        }
//        if( m_repo instanceof GRdfSchemaRepository ) {
//            GRdfSchemaRepository repo = ((GRdfSchemaRepository)m_repo);
//            GGraph graph = repo.getGraph();
//            repo.getProperties();
//            metricsWriter.write("\n");
//        }
        
        metricsWriter.write("filesLoaded, ");
        metricsWriter.write("elapsedTime, ");
        metricsWriter.write("transactionsPerSecond, ");
        metricsWriter.write("loadTime, ");
        metricsWriter.write("commitTime, ");
        metricsWriter.write("transactionTime, ");
        metricsWriter.write("toldTriples, ");
        metricsWriter.write("toldTriplesPerSecond1, ");
        metricsWriter.write("toldTriplesPerSecond2, ");
        metricsWriter.write("totalLoadTime, ");
        metricsWriter.write("totalCommitTime, ");
        metricsWriter.write("totalTransactionTime, ");
        metricsWriter.write("avgLoadTime, ");
        metricsWriter.write("avgCommitTime, ");
        metricsWriter.write("avgTransactionTime, ");
        metricsWriter.write("avgToldTriplesPerSec1, ");
        metricsWriter.write("avgToldTriplesPerSec2, ");
        metricsWriter.write("triplesAdded, " );
        metricsWriter.write("inferencesAdded, ");
        metricsWriter.write("proofsAdded, ");
        metricsWriter.write("urisAdded, ");
        metricsWriter.write("bnodesAdded, ");
        metricsWriter.write("literalsAdded, ");
        metricsWriter.write("totalToldTriples, ");
        metricsWriter.write("triplesInStore, ");
        metricsWriter.write("inferencesInStore, ");
        metricsWriter.write("proofsInStore, ");
        metricsWriter.write("urisInStore, ");
        metricsWriter.write("bnodesInStore, ");
        metricsWriter.write("literalsInStore, ");
        metricsWriter.write("totalMemory, ");
        metricsWriter.write("error, ");
        metricsWriter.write("filename\n");
        
    }
    
    /**
     * Formatting useful for integers and floating point values that need to be
     * rounded to integers. If the value is in milliseconds, and you want to
     * write it in seconds then first divide by 1000. If the value is units per
     * millisecond and you want to write units per second, then compute and
     * format <code>units/milliseconds*1000</code>.
     */
    final NumberFormat nf;

    /**
     * Formatting useful for floating point values with at most three digits
     * after the decimal (used for the transactions per second column - a non-
     * integer value is used to smooth out the curve when transactions per
     * second is plotted against the #of documents loaded).
     */
    final NumberFormat fpf;

    /**
     * Computes units/second given units and milliseconds.
     * 
     * @param units
     *            The units, e.g., the #of triples loaded.
     * @param ms
     *            The milliseconds.
     * 
     * @return Units/seconds, e.g., the #of triples loaded per second. If <i>ms</i>
     *         is zero(0) then this method returns zero.
     */
    public double getUnitsPerSecond(long units,long ms) {
    
        if( ms == 0 ) return 0d;
        
        return (((double)units)/ms)*1000;
        
    }
    
    /**
     * Start of the run.
     */
    final long beginRunTime = System.currentTimeMillis();

    /**
     * #of files loaded so far.
     */
    int filesLoaded = 0;
    
    long totalLoadTime = 0L;
    long totalCommitTime = 0L;
    long totalTransactionTime = 0L;
    
    /**
     * Total #of told triples processed (there may be fewer in the repository
     * since triples may dupicate one another).
     */
    long totalToldTriples = 0L;
    long totalInferenceCount = 0L;
    long totalProofCount = 0L;

    private DataLoader dataLoader;

    /**
     * Sets up the store.
     * <p>
     * Note: since this is run as a proxy test case you have to specify
     * properties on the command line using -Dproperty=value. Default properties
     * are inherited from the class specified by the required -DtestClass=...
     * property.
     * 
     * <pre>
     * 
     * Note: this turns off sameAs processing.
     * 
     * properties.setProperty(InferenceEngine.Options.RDFS_ONLY, &quot;true&quot;);
     * 
     * Note: this turns off the justification chains (useful for estimating benefit of magic sets).
     * 
     * properties.setProperty(InferenceEngine.Options.JUSTIFY, &quot;false&quot;);
     * 
     * Note: this turns off truth maintenance during data load (see tearDown for database at once closure).
     * 
     * properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());
     * 
     * Note: this turns off commit - useful for guaging the uninterrupted write rate and effects of group commit.
     * 
     * properties.setProperty(DataLoader.Options.COMMIT, CommitEnum.None.toString());
     * 
     * Note: disables auto-flush of the statement buffer after each document
     * allowing the data loader to approximate the behavior of a large document
     * load.
     * 
     * properties.setProperty(DataLoader.Options.FLUSH, "false");
     * 
     * </pre>
     */
    public void setUp() throws Exception
    {
        
        super.setUp();

        dataLoader = store.getDataLoader();
                
        writeMetricsLogHeaders();
        
    }
    
    public void tearDown() throws Exception
    {

        if(!dataLoader.getFlush()) {
            
            dataLoader.flush();
            
        }
        
        if(true) {

            log.warn("Computing closure of the database.");
            
            log.warn("database at once closure:\n"
                    + store.getInferenceEngine().computeClosure(null));
            
        }
        
        // final commit on the store.
        
        log.warn("Final commit on the store.");
        
        store.commit();

        log.warn("Statistics after final commit on the store.");
        
        usage();
        
        try {
        
            metricsWriter.flush();

            metricsWriter.close();

        } catch (Throwable t) {
            
            log.warn("Problem closing/flushing metrics file: " + t, t);
            
        }
        
        super.tearDown();

        dataLoader = null;
        
    }

    /**
     * Writes information about the store and the indices on stderr and the
     * {@link ResourceManager#log}.
     */
    private void usage() {
        
        String usage = store.usage();
        
        System.err.println(usage);
        
        ResourceManager.log.warn(usage);

    }
    
    /**
     * Load files in the documents directory (recursively scanning
     * subdirectories).
     */
    public void loadFiles() throws IOException {
        
        loadFilesInDirectory( documentDir );
        
    }
    
    protected void loadFilesInDirectory(File dir) throws IOException {

        File[] files = dir.listFiles();
        
        for( int i=0; i<files.length; i++ ) {

            /*
             * When non-zero this will limit #of files loaded.
             */
            if (maxFiles != 0 && filesLoaded >= maxFiles) {

                return;
                
            }
            
            File file = files[ i ];
    
            if( file.isHidden() ) continue;
            
            if( file.isDirectory() ) {
                
                if( file.getName() == "." || file.getName()==".." ) continue;
                
                loadFilesInDirectory( file );
                
            } else {
            
                loadFile( file );
//                loadData( file.toString() );
                
            }
            
        }
        
    }

    public void loadFile(File file) throws IOException {
        
        logMetrics( new Trial(file.toString() ) );
        
    }
    
    /**
     * Encapsulates the metrics reported for a single file load.  The #of inferences
     * and proofs are reported only for the GOM SAIL.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */

    public class Trial {
        
        static final String baseURL = "";

        /**
         * The name of the processed file.
         */
        final String file;

        /**
         * The #of told triples processed in the file.
         */
        long toldTriples = 0;

        /**
         * <code>null</code> iff an no error occurred and otherwise the
         * {@link Throwable} object.
         */
        Throwable error;

        /**
         * The timestamp immediately before the transaction begins.
         */
        final long begin;

        /**
         * This is the time required to load the triples exclusive of the
         * startup and commit time for the transaction.
         */
        final long loadTime;

        /**
         * The time required to commit the transaction (actually includes the
         * transaction startup overhead as well, but that is discounted since it
         * should be very low latency).
         */
        final long commitTime;

        /**
         * This is the elapsed time for the entire transaction in which the file
         * was loaded. It includes parsing the file, loading the data into the
         * store, and the time required to perform the transaction commit.
         */
        final long transactionTime;

        /**
         * The #of statements added to the repository during the trial (axioms,
         * told triples, and inferences) (iff GOM SAIL otherwise zero(0)).
         */
        int statementsAdded;

        /**
         * The #of inferences added to the repository during the trial (iff GOM
         * SAIL).
         */
        int inferencesAdded;

        /**
         * The #of proof objects added to the repository during the trial (iff
         * GOM SAIL).
         */
        int proofsAdded;

        /**
         * The #of URIs added to the repository during the trail (iff GOM SAIL
         * otherewise zero(0)).
         */
        int urisAdded;

        /**
         * The #of blank nodes added to the repository during the trail (iff GOM
         * SAIL).
         */
        int bnodesAdded;

        /**
         * The #of literals added to the repository during the trail (iff GOM
         * SAIL).
         */
        int literalsAdded;

        /**
         * The #of statements in the repository after the trial (axioms, told
         * triples, and inferences) (iff GOM SAIL otherewise zero(0)).
         */
        int statementCount1;

        /**
         * The #of inferences in the repository after the trial (iff GOM SAIL
         * otherewise zero(0)).
         */
        int inferenceCount1;

        /**
         * The #of proofs objects in the repository after the trial (iff GOM
         * SAIL otherewise zero(0)).
         */
        int proofCount1;

        /**
         * The #of URIs in the repository after the trial (iff GOM SAIL
         * otherewise zero(0)).
         */
        int uriCount1;

        /**
         * The #of blank nodes in the repository after the trial (iff GOM SAIL
         * otherewise zero(0)).
         */
        int bnodeCount1;

        /**
         * The #of literals in the repository after the trial (iff GOM SAIL
         * otherewise zero(0)).
         */
        int literalCount1;
        
        /**
         * Load an RDF/XML file into the repository and write statistics on the
         * metrics log. If an error occurs during load then the error will be
         * logged and the #of statements processed will be set to zero for that
         * document.
         */

        public Trial(String file) {
            
            assert file != null;
            
            this.file = file;

            // #of statements in the repository (before the trial).
            final int statementCount0 = store.getStatementCount();
//            // @todo inference count.
//            final int inferenceCount0 = 0;
            // proof count.
            final int proofCount0 = store.getJustificationCount();
            // uri count.
            final int uriCount0 = store.getURICount();
            // bnode count.
            final int bnodeCount0 = store.getBNodeCount();
            // literal count.
            final int literalCount0 = store.getLiteralCount();

//            final MetricsListener listener = new MetricsListener();

            // time before the transaction starts.
            begin = System.currentTimeMillis();

            LoadStats loadStats;
            
            try {

                loadStats = dataLoader.loadData(file, baseURL, RDFFormat.RDFXML);
                
//                loadStats = store
//                        .loadData(file, "", RDFFormat.RDFXML,
//                                false/* verifyData */, true/* commit */);
                
//                InputStream rdfStream = new BufferedInputStream(
//                        new FileInputStream(file));
//
//                RdfAdmin admin = new RdfAdmin(m_repo);
//
//                toldTriples = admin.addRdfModel(rdfStream, baseURL,
//                        listener, RDFFormat.RDFXML, validate);
////              new StdOutAdminListener(), RDFFormat.RDFXML, validate);
//
//                rdfStream.close();

            } catch (Throwable t) {
                
                error = t;
                
                loadStats = new LoadStats();
                
            }

            /*
             * #of explicit statements loaded.
             */
            toldTriples = loadStats.toldTriples;
            
            /*
             * This is the elapsed time for the entire transaction in which the file
             * was loaded. It includes parsing the file, loading the data into the
             * store, and the time required to perform the transaction commit.
             */
//            transactionTime = System.currentTimeMillis() - begin;
            transactionTime = loadStats.totalTime;

            /*
             * This is the time required to load the triples exclusive of the
             * startup and commit time for the transaction.
             */
            loadTime = loadStats.loadTime;

            /*
             * A pragmatic estimate of the commit time that assumes the transaction
             * start time is zero. 
             */
            commitTime = loadStats.commitTime;
//            commitTime = transactionTime - loadTime;

            /*
             * Touch up the elapsed load time statistics on the base class.
             */
            elapsedLoadTime += transactionTime;

            /*
             * The #of statements in the repository (after the trial).
             */
            
            // total statement count (axioms + inferences + told triples)
            statementCount1 = store.getStatementCount();
//            // @todo inference count.
//            inferenceCount1 = 0;
            // proof count.
            proofCount1 = store.getJustificationCount();
            // uri count.
            uriCount1 = store.getURICount();
            // bnode count.
            bnodeCount1 = store.getBNodeCount();
            // literal count.
            literalCount1 = store.getLiteralCount();

            statementsAdded = statementCount1 - statementCount0;
//            inferencesAdded = inferenceCount1 - inferenceCount0;
            inferencesAdded = loadStats.closureStats.nentailments;
//            int explicitAdded   = statementsAdded - inferencesAdded;
            proofsAdded     = proofCount1 - proofCount0;
            urisAdded       = uriCount1 - uriCount0;
            bnodesAdded     = bnodeCount1 - bnodeCount0;
            literalsAdded   = literalCount1 - literalCount0;
            
            if(error!=null) {
                
                error.printStackTrace(System.err);
                
            }

        }
        
    }
   
//    /**
//     * By virtue of the the manner in which {@link SesameUpload} is implemented,
//     * the methods {@link #transactionStart()} and {@link #transactionEnd()} are
//     * invoked after the transaction has been started and before it has been
//     * committed, respectively. This means that we can use those events to
//     * report the time load the statements into the kb as distinct from the time
//     * required to perform the commit.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * 
//     * @todo we can now this stuff directly for our {@link LocalTripleStore} and that
//     *       will pose fewer problems than this integration point with Sesame,
//     *       which works only for a specific patched version of the Sesame code.
//     */
//    public static class MetricsListener extends StdOutAdminListener
//    {
//        
//        private long startTime;
//        private long elapsed = -1L;
//        
//        public MetricsListener() {
//            super();
//        }
//        
//        public void transactionStart() {
//            startTime = System.currentTimeMillis();
//            super.transactionStart();
//        }
//        
//        public void transactionEnd() {
//            elapsed = System.currentTimeMillis() - startTime;
//            super.transactionEnd();
//        }
//
//        /**
//         * The time required to load the statements during some transaction
//         * exclusive of the startup and commit costs for that transaction.
//         * 
//         * @exception UnsupportedOperationException
//         *                if {@link SesameUpload} has not been patched to invoke
//         *                {@link #transactionStart()} (after the transaction is
//         *                started) and {@link #transactionEnd()} (before the
//         *                transaction is started).
//         */
//
//        public long getLoadTime() {
//        
//            if(startTime == 0L) {
//                throw new UnsupportedOperationException(
//                        "You MUST patch SesameUpload to invoke transactionStart()!");
//            }
//            
//            if(elapsed == -1L) {
//                throw new UnsupportedOperationException(
//                        "You MUST patch SesameUpload to invoke transactionEnd()!");
//            }
//            
//            return elapsed;
//            
//        }
//        
//    }

    /**
     * <p>
     * This program is configured using a Properties resource in the same
     * package with the same basename as this class. Several properties have
     * semantics that are inherited from the base class or the openrdf or other
     * repository. See {@link RuntimeOptions}.
     * <p>
     * A log file is written containing metrics for each file loaded into the
     * repository. Those metrics include the name of the file, the #of
     * statements in the file, the start time for the load operation, the end
     * time for the load operation, and an error message if the load failed for
     * any reason.
     * </p>
     * 
     * @param args
     *            <strong>ignored</strong>
     * 
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        TestMetrics testMetrics = new TestMetrics("TestMetrics");
        
        testMetrics.setUp();

        if( testMetrics.documentOntology != null ) {

            testMetrics.loadFile( testMetrics.documentOntology );
            
//            testMetrics.loadData( ""+testMetrics.documentOntology );

        }
        
        try {

            // load the data files.
            testMetrics.loadFiles();
            
        } finally {
            
            // tear down the test.
            testMetrics.tearDown();
            
        }
        
    }
    
}
