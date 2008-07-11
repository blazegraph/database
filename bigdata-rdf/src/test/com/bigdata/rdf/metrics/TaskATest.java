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
 * Created on Nov 30, 2005
 */
package com.bigdata.rdf.metrics;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rules.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.rules.InferenceEngine.Options;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.store.DataLoader.CommitEnum;

/**
 * FIXME refactor to use the ExperimentDriver.  This let me collect better
 * data.  It will also give me better control over which conditions to run
 * and which data sets to run as part of the condition and make it easier to
 * analyze the data by condition.  However the TestMetrics class will also
 * need to collect detailed traces per condition, so it should save the name
 * of the trace file as part of the condition result.
 * 
 * @todo update javadoc.
 * @todo write some useful summary data into a file.
 * 
 * <p>
 * Class obtains metrics on access path usage and the costs of an ontology for
 * one or more datasets. The test case provides a driver that creates a new
 * repository with the appropriate properties to enable the data collection,
 * loads the data, shuts down the repository, copies the data to a file named
 * for the data set whose characteristics we are gathering, and then deletes the
 * store files.
 * </p>
 * <p>
 * Note: This is not really designed as a JUnit test and does not expose any
 * suitable public constructors. {@link #main( String[] )} is responsible for
 * which tests are actually run. This class extends
 * {@link AbstractMetricsTestCase} so that we can leverage the infrastructure
 * of the latter. You MUST specify "-DrepositoryClass=" on the command line and
 * give the name of an {@link RdfSchemaRepository} implementation class.
 * </p>
 * <p>
 * This generates the following output files:
 * <dl>
 * <dt><i>name </i>-status.txt</dt>
 * <dd>Either "SUCCESS" or "FAILURE" and a stack trace.</dd>
 * <dt><i>name </i>-stats.csv</dt>
 * <dd>Various statistics about the loaded data. This is a comma delimited
 * file. It will include access path data if that option is selected and is
 * supported by the repository.</dd>
 * <dt><i>name </i>-ontologyCosts.csv</dt>
 * <dd>Ontology cost matrix (comma delimited). This option requires that proof
 * objects are stored and that an ontoloty costs report is requested. This
 * option is only supported by the GOM repositories.</dd>
 * </dl>
 * </p>
 * 
 * @todo Add output directory parameter?
 * 
 * @author thompsonbry
 */
public class TaskATest
   extends AbstractMetricsTestCase
{

    final FileAndBaseURL[] _sources;

    /** ctor used by junit. */
    public TaskATest() {
        super();
        _sources = null;
    }

    /** ctor used by junit. */
    public TaskATest(String name) {
        
        super(name);
        
        _sources = null;
        
    }
    
    /** ctor used to run a particular datasource. */
    public TaskATest( String name, String sources, File outDir )
    {
        
        super( name );
        
        if( name == null || sources  == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        _sources = getSources( sources );
        
        System.err.println( "\n***\n*** "+name+"\n***\n");
        
    }

    /**
     * Verify that each of the RDF source files exists.
     */

    public void assertFilesExist()
        throws IOException
    {

        String filename[] = FileAndBaseURL.getFileNames( _sources );
        
        for( int i=0; i<filename.length; i++ ) {
  
            String file = filename[ i ];
            
//            InputStream is = getClass().getResourceAsStream(file);
//        
//            assertNotNull( "Could not read: file="+file, is );
            
//            is.close();
            
            assertTrue(new File(file).exists());
            
//            File file = new File( filename[ i ] );
//            assertTrue( "file does not exist: "+file, file.exists() );
            
        }
        
    }

    public void setUp() throws Exception
    {
        
        super.setUp();
        
    }
    
    public void tearDown() throws Exception {
        
//        // Before shutdown.
//        if( reportOntologyCosts ) {
//
//            writeOntologyCosts();
//            
//        }
        
        super.tearDown();

        // After shutdown.
        writeStatistics();
        
    }

    /**
     * Write general statistics (includes access path data).
     */
    public void writeStatistics()
    {
    }
    
    /**
     * Load each RDF source in turn.
     * 
     * @param computeEntailments
     *            When true, the RDF(S) closure of the store will be computed.
     * 
     * @return The elasped time to load the data and perform closure.
     */
    public long loadData() throws IOException {

        /*
         * Configure the DataLoader and InferenceEngine.
         */
        
        Properties properties = new Properties(getProperties());

        /*
         * Note: the interesting comparisons are None (close the database once
         * the data are loaded into the database) and incremental (load each
         * file into a temporary store and then close the temporary store
         * against the databaser).
         * 
         * Note: these data sets are getting loaded into distinct databases.
         * Wordnet D+S and S+D are the only exceptions, where we load two files
         * into a single database.
         * 
         * @todo an interesting comparison is to load all data into a single
         * database and then compute its closure.
         * 
         * @todo if the various uniprot data sets are related (and I am not sure
         * about that) then they should be loaded as a single data set.
         */
        
        // whether and when to compute the closure.
//        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.Incremental.toString());
//        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.Batch.toString());
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());

        // after every set of resources loaded.
        properties.setProperty(DataLoader.Options.COMMIT,CommitEnum.Batch.toString());

        // Note: this turns off sameAs processing.
        properties.setProperty(Options.RDFS_ONLY, "true");

        // generate justifications for TM.
//        properties.setProperty(Options.JUSTIFY, "true");
        properties.setProperty(Options.JUSTIFY, "false");

        // forward chain (x rdf:type rdfs:Resource)
//        properties.setProperty(
//                Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "true");
        properties.setProperty(
                Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "false");

        // choice of the forward closure method.
//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());

        /*
         * load (and optionally close) the data.
         */
        
        long begin = System.currentTimeMillis();

        String[] resource = FileAndBaseURL.getFileNames(_sources);
        
        String[] baseURL = FileAndBaseURL.getBaseURLs(_sources);
        
        RDFFormat[] rdfFormat = FileAndBaseURL.getFormats(_sources);

        DataLoader dataLoader = new DataLoader(properties,store);
        
        loadStats = dataLoader.loadData(resource, baseURL, rdfFormat);
        
        if(false && dataLoader.getClosureEnum()==ClosureEnum.None) {
            
            /*
             * When None is selected above we compute the full closure of the
             * database after the file(s) have been loaded. In this case the
             * told triples are written directly on the database rather than
             * onto a temporary store and the closure of the database is
             * computed - this is the fastest way to close a large data set.
             */
            
            loadStats.closureStats.add( dataLoader.doClosure() );
            
        }

        // release resources.
        dataLoader = null;
        
        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("*** Elapsed total time: " + (elapsed/ 1000) + "s\n");

        return elapsed;

    }
    
    /**
     * Set by {@link #loadData()}.
     */
    protected LoadStats loadStats;
    
    /**
     * Datasets to load - in order by size. Each dataset described by three (3)
     * values.
     * <ol>
     * 
     * <li>Description of the dataset.</li>
     * 
     * <li>Name assigned to the dataset - it is the base filename for the
     * dataset files whenever possible.</li>
     * 
     * <li>A parameter which can be parsed into a sequence of RDF files to be
     * loaded in the repository. If the data set requires a baseURI then it MUST
     * be specified here. </li>
     * 
     * </ol>
     */
    public static final String[] all_sources = new String[] {
            
            "Natural Protected Areas in Spain",
            "parques",
            "-rdf ../rdf-data/parques.daml http://www.sia.eui.upm.es/onto/parques.daml",

            /* dc:source=http://www.daml.org/ontologies/76 */
            "Food/Wine Ontology",
            "wines.daml",
            "-rdf ../rdf-data/wines.daml http://cvs.sourceforge.net/viewcvs.py/instancestore/instancestore/ontologies/Attic/wines.daml?rev=1.2",
            
            "Semantic Web Research Community",
            "sw_community",
            "-rdf ../rdf-data/sw_community.owl http://www.semanticweb.gr/sw_community/sw_community.owl",
            
            "Information about Russia from Lockheed Martin",
            "russiaA",
            "-rdf ../rdf-data/russiaA.rdf http://www.atl.external.lmco.com/projects/ontology/ontologies/russia/russiaA.rdf",
            
            "University of Georgia Semantic web-related bibliography",
            "Could_have_been",
            "-rdf ../rdf-data/Could_have_been.rdf http://www.arches.uga.edu/~vstaub/GlobalInfoSys/project/ontology/Could_have_been.rdf",
            
            "IPTC Subject Reference",
            "iptc-srs",
            "-rdf ../rdf-data/iptc-srs.rdfs http://nets.ii.uam.es/neptuno/iptc/iptc-srs.rdfs",
            
            "Hydrological and watershed data",
            "hu",
            "-rdf ../rdf-data/hu.xml http://loki.cae.drexel.edu/~how/HydrologicUnits/2003/09/hu",
            
            "Uniprot Protein Sequence Ontology",
            "core",
            "-rdf ../rdf-data/core.owl http://www.isb-sib.ch/%7Eejain/rdf/data/core.owl",

            "Uniprot Enzymes Data",
            "enzymes",
            "-rdf ../rdf-data/enzymes.rdf", // http://www.isb-sib.ch/%7Eejain/rdf/data/enzymes.rdf.gz

            "Alibaba v4.1",
            "alibaba_v41",
            "-rdf ../rdf-data/alibaba_v41.rdf",

//            @todo this is gone, get the NLM mesh data instead?
//            "Medical Subject Headings",
//            "mesh",
//            "-rdf /mesh.rdf", // http://www.isb-sib.ch/%7Eejain/rdf/data/mesh.rdf.gz
            
            "Wordnet Schema + Nouns",
            "wordnetS+D",
            "-rdf ../rdf-data/wordnet-20000620.rdfs -rdf ../rdf-data/wordnet_nouns-20010201.rdf",

            "Wordnet Nouns + Schema",
            "wordnetD+S",
            "-rdf ../rdf-data/wordnet_nouns-20010201.rdf -rdf ../rdf-data/wordnet-20000620.rdfs",

            "NCI Oncology (old)",
            "nciOncology",
            "-rdf ../rdf-data/nciOncology.owl",

            "NCI Oncology (new)", // ftp://ftp1.nci.nih.gov/pub/cacore/EVS/Thesaurus_06.04d.OWL.zip
            "Thesaurus",
            "-rdf ../rdf-data/Thesaurus.owl",

        /* starts to swap heavily on the workstation when run as:
         * 
         * -server -Xmx500m -DtestClass=com.bigdata.rdf.store.TestLocalTripleStore
         */ 
//          Very large data set with large ontology.
          "Uniprot Protein Sequence Taxonomy",
          "taxonomy",
          "-rdf ../rdf-data/taxonomy.rdf", // http://www.isb-sib.ch/%7Eejain/rdf/data/taxonomy.rdf.gz
            
//          // cyc has a lot of subClassOf stuff.
          "OpenCyc",
          "cyc",
          "-rdf ../rdf-data/cyc.xml http://www.cyc.com/2004/06/04/cyc",

    };

    public void test() throws FileNotFoundException {
  
        int nok = 0;
        int nruns = all_sources.length / 3;
        
        if( nruns * 3 != all_sources.length ) {
            
            throw new AssertionError( "There must be three entries for each dataset (desc, name, sources)." );
            
        }
        
        Throwable[] errors = new Throwable[ nruns ];

        /*
         * Output directory and file names for this test run.
         */
        
        File outDir = new File( "TaskA" );
        
        if( ! outDir.exists() ) {
            
            outDir.mkdirs();
            
        }

        LoadStats[] loadStats = new LoadStats[ all_sources.length ];
        
        for( int i=0, run=0; i<all_sources.length; i+=3, run++ ) {
            
            String desc = all_sources[ i ];
            String name = all_sources[ i + 1 ];
            String sources = all_sources[ i + 2 ];
            
            TaskATest test = new TaskATest( name, sources, outDir );

            File status = new File( outDir, name+"-status.txt" );
            
            PrintWriter w = new PrintWriter( new BufferedOutputStream( new FileOutputStream( status ) ) );
            
            try {
                test.setUp();
                try {
                    test.assertFilesExist();
                    test.loadData();
                    loadStats[run] = test.loadStats;
                    w.println( "SUCCESS" );
                    nok++;
                    errors[ run ] = null;
                }
                finally {
                    try {
                        test.tearDown();
                    }
                    catch( Throwable t2 ) {
                        log.warn( "Could not tear down test: "+name, t2 );
                    }
                }
            }
            catch( Throwable t ) {
                log.error( name+": "+t.getMessage(), t );
                w.println( "FAILURE" );
                t.printStackTrace( w );
                errors[ run ] = t;
            }
            w.flush();
            w.close();
            
        }

        System.out.println( "\n\n\n"+nok+" out of "+nruns+" Ok.");
        
        System.out
                .println("name, status"+
                        ", toldTriples, loadTime(s), toldTriplesPerSecond"+
                        ", entailments, closureTime(s), entailmentsPerSecond"+
                        ", commitTime(ms)"+
                        ", totalTriplesPerSecond"
                        );
        
        for( int run=0; run<nruns; run++ ) {
            
            // MAY be null.
            ClosureStats closureStats = loadStats[run].closureStats;
            
            
            // Explicit + (Entailments = Axioms + Inferred)
            final long totalTriples = loadStats[run].toldTriples
                    + (closureStats!=null?closureStats.mutationCount : 0);

            // loadTime + closureTime + commitTime.
            final long totalTime = loadStats[run].loadTime
                    + (closureStats != null ? closureStats.elapsed : 0)
                    + loadStats[run].commitTime;
            
            System.out.println( all_sources[ run * 3 ]+ ", " +
                    ( errors[ run ] == null
                      ? "Ok"
                            +", "+loadStats[run].toldTriples
                            +", "+loadStats[run].loadTime/1000
                            +", "+tps(loadStats[run].toldTriples,loadStats[run].loadTime)
                            +", "+(closureStats!=null?closureStats.mutationCount:"")
                            +", "+(closureStats!=null?closureStats.elapsed/1000:"")
                            +", "+(closureStats!=null?tps(closureStats.mutationCount,closureStats.elapsed):"")
                            +", "+loadStats[run].commitTime
                            +", "+tps(totalTriples,totalTime)

                      : errors[ run ].getMessage()
                      )
                    );
            
        }
        
    }
    
    /**
     * Returns triples/second.
     * 
     * @param ntriples
     * @param elapsed
     * @return
     */
    private String tps(long ntriples, long elapsed) {
        
        if(elapsed==0) {
            
            return "N/A";
            
        }
        
        return ""+(long) (ntriples * 1000d / elapsed);
        
    }
    
}
