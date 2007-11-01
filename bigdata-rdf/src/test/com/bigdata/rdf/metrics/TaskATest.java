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

import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfSchemaRepository;

import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.inf.InferenceEngine.Options;
import com.bigdata.rdf.rio.LoadStats;
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
 * {@link AbstractRepositoryTestCase} so that we can leverage the infrastructure
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
    public long loadData() throws IOException, UpdateException {

        /*
         * Configure the DataLoader and InferenceEngine.
         */
        
        Properties properties = new Properties(getProperties());

        // whether and when to compute the closure.
//        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.Incremental.toString());
//        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.Batch.toString());
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());

        // after every set of resources loaded.
        properties.setProperty(DataLoader.Options.COMMIT,CommitEnum.Batch.toString());

        // generate justifications for TM.
        properties.setProperty(Options.JUSTIFY, "true");

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
        
        if(true && dataLoader.getClosureEnum()==ClosureEnum.None) {
            
            // compute the full closure of the database.
            
            loadStats.closureStats = dataLoader.doClosure();
            
        }

        // release resources.
        dataLoader.close();
        
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
            "-rdf data/parques.daml http://www.sia.eui.upm.es/onto/parques.daml",

            /* dc:source=http://www.daml.org/ontologies/76 */
            "Food/Wine Ontology",
            "wines.daml",
            "-rdf data/wines.daml http://cvs.sourceforge.net/viewcvs.py/instancestore/instancestore/ontologies/Attic/wines.daml?rev=1.2",
            
            "Semantic Web Research Community",
            "sw_community",
            "-rdf data/sw_community.owl http://www.semanticweb.gr/sw_community/sw_community.owl",
            
            "Information about Russia from Lockheed Martin",
            "russiaA",
            "-rdf data/russiaA.rdf http://www.atl.external.lmco.com/projects/ontology/ontologies/russia/russiaA.rdf",
            
            "University of Georgia Semantic web-related bibliography",
            "Could_have_been",
            "-rdf data/Could_have_been.rdf http://www.arches.uga.edu/~vstaub/GlobalInfoSys/project/ontology/Could_have_been.rdf",
            
//            @todo can not find RDF version.
//            "REIS Technical Manual from the Data Consortium",
//            "REISDocuments",
//            "-rdf /REISDocuments.rdf http://www.dataconsortium.org/REISDocuments.rdf",
            
            "IPTC Subject Reference",
            "iptc-srs",
            "-rdf data/iptc-srs.rdfs http://nets.ii.uam.es/neptuno/iptc/iptc-srs.rdfs",
            
            "Hydrological and watershed data",
            "hu",
            "-rdf data/hu.xml http://loki.cae.drexel.edu/~how/HydrologicUnits/2003/09/hu",
            
            "Uniprot Protein Sequence Ontology",
            "core",
            "-rdf data/core.owl http://www.isb-sib.ch/%7Eejain/rdf/data/core.owl",

            "Uniprot Enzymes Data",
            "enzymes",
            "-rdf data/enzymes.rdf", // http://www.isb-sib.ch/%7Eejain/rdf/data/enzymes.rdf.gz

            "Alibaba v4.1",
            "alibaba_v41",
            "-rdf data/alibaba_v41.rdf",
            
//            @todo this is gone, get the NLM mesh data instead?
//            "Medical Subject Headings",
//            "mesh",
//            "-rdf /mesh.rdf", // http://www.isb-sib.ch/%7Eejain/rdf/data/mesh.rdf.gz
            
            "Wordnet Schema + Nouns",
            "wordnetS+D",
            "-rdf data/wordnet-20000620.rdfs -rdf data/wordnet_nouns-20010201.rdf",

            "Wordnet Nouns + Schema",
            "wordnetD+S",
            "-rdf data/wordnet_nouns-20010201.rdf -rdf data/wordnet-20000620.rdfs",

            // @todo do closure times for cyc - it has a lot of subClassOf stuff.
//            "OpenCyc",
//            "cyc",
//            "-rdf data/cyc.xml http://www.cyc.com/2004/06/04/cyc",

            "NCI Oncology (old)",
            "nciOncology",
            "-rdf data/nciOncology.owl",

            "NCI Oncology (new)", // ftp://ftp1.nci.nih.gov/pub/cacore/EVS/Thesaurus_06.04d.OWL.zip
            "Thesaurus",
            "-rdf data/Thesaurus.owl",

        /*
         * @todo I have seen what appears to be a tight loop emerge during
         * closure when this is run along with all of the others above, but I
         * have also seen it run to completion just fine when run by itself.
         * Perhaps there is a fence post somewhere in the closure logic that
         * occasionally results in non-terminating loops?
         */
         // Very large data set with large ontology.
//          "Uniprot Protein Sequence Taxonomy",
//          "taxonomy",
//          "-rdf data/taxonomy.rdf" // http://www.isb-sib.ch/%7Eejain/rdf/data/taxonomy.rdf.gz
            
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
                    + (closureStats!=null?closureStats.nentailments : 0);

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
                            +", "+(closureStats!=null?closureStats.nentailments:"")
                            +", "+(closureStats!=null?closureStats.elapsed/1000:"")
                            +", "+(closureStats!=null?tps(closureStats.nentailments,closureStats.elapsed):"")
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
    private long tps(long ntriples, long elapsed) {
        
        return ((long)( ((double)ntriples) / ((double)elapsed) * 1000d ));
        
    }
    
}
