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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfSchemaRepository;

import com.bigdata.rdf.rio.LoadStats;

/**
 * @todo update javadoc.
 * @todo run with and without inference.
 * @todo try with the bulk loader as well as the presort loader.
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
 * @todo Run against both CTC and CWEB GOM stores with and without schema
 *       support and with incremental vs all at once closure as a means of
 *       testing rdf-generic on a wider variety of RDF datasets.
 * 
 * @todo Write ontology costs iff proofs are stored and ontology costs are
 *       requested. Raise error if ontology costs are requested without proofs
 *       being stored.
 * 
 * @author thompsonbry
 */

public class TaskATest
   extends AbstractMetricsTestCase
{

    
    final FileAndBaseURI[] _sources;

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

    public void testFilesExist()
        throws IOException
    {

        String filename[] = FileAndBaseURI.getFileNames( _sources );
        
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
    
    public void tearDown()
    {
        
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

    public void testOntology() throws Exception
    {

        /*
         * Load the data. Sets some instance variables from various counters.
         */
        loadData();
        
    }

    /**
     * Write general statistics (includes access path data).
     */
    public void writeStatistics()
    {

//        File file = statisticsFile;
//        
//        System.err.println( "Writing: "+file );
//        
//        try {
//
//            Writer w = new FileWriter( file );
//        
//            m_stats.writeOn( w );
//            
//            w.flush();
//            
//            w.close();
//            
//            System.err.println( "Wrote: "+file );
//            
//        }
//
//        catch( IOException ex ) {
//            
//            log.error( "Could not write: "+file, ex );
//            ex.printStackTrace( System.err );
//            
//        }
        
    }
    
//    /**
//     * Write ontology costs.
//     * <p>
//     * 
//     * Note: Proof objects MUST be stored to compute the ontology costs.
//     * <p>
//     */
//
//    public void writeOntologyCosts()
//    {
//
//        File file = ontologyFile;
//
//        System.err.println( "Writing: "+file );
//        
//        try {
//
//            Writer w = new FileWriter( file );
//        
//            OntologyCostsReporter.reportOntologyCosts((GRdfSchemaRepository)m_repo, w );
//            
//            w.flush();
//            
//            w.close();
//            
//            System.err.println( "Wrote: "+file );
//        }
//
//        catch( Throwable ex ) {
//            
//            log.error( "Could not write: "+file, ex );
//            ex.printStackTrace( System.err );
//            
//        }
//
//    }
    
    /**
     * Load each RDF source in turn.
     * 
     * @return The elasped time to load the data and perform closure.
     */
    public long loadData() throws IOException, UpdateException {

        long begin = System.currentTimeMillis();

        String filename[] = FileAndBaseURI.getFileNames(_sources);
        String baseURI[] = FileAndBaseURI.getBaseURIs(_sources);
        
        for(int i=0; i<filename.length; i++) {
         
            loadStats = store
                    .loadData(new File(filename[i]), baseURI[i],
                            RDFFormat.RDFXML, false/* verifyData */, true/* commit */);
            
        }

        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("\n*** Elapsed time: " + (elapsed / 1000) + "s\n");

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

            // Note: CTC runs out of Java heap space when using the default heap.
            "OpenCyc",
            "cyc",
            "-rdf data/cyc.xml http://www.cyc.com/2004/06/04/cyc",

            "NCI Oncology (old)",
            "nciOncology",
            "-rdf data/nciOncology.owl",

            "NCI Oncology (new)", // ftp://ftp1.nci.nih.gov/pub/cacore/EVS/Thesaurus_06.04d.OWL.zip
            "Thesaurus",
            "-rdf data/Thesaurus.owl",

        /*
         * CTC ran out of heap space with default heap - [status ] : Processed
         * 818,938 statements in 10800 seconds; processing continues
         */
         // Very large data set with large ontology.
          "Uniprot Protein Sequence Taxonomy",
          "taxonomy",
          "-rdf data/taxonomy.rdf" // http://www.isb-sib.ch/%7Eejain/rdf/data/taxonomy.rdf.gz
            
    };

    public static void main( String[] args )
        throws Exception
    {
  
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
                    test.testFilesExist();
                    test.testOntology();
                    loadStats[run] = test.loadStats;
//                    test.testReadPerformance();
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
        
        System.out.println("name, status, triplesPerSecond, loadTime(s), commitTime(ms)");
        
        for( int run=0; run<nruns; run++ ) {
            
            System.out.println( all_sources[ run * 3 ]+ ", " +
                    ( errors[ run ] == null
                      ? "Ok, "
                            + loadStats[run].triplesPerSecond() + ", "
                            + loadStats[run].loadTime / 1000 + ", "
                            + loadStats[run].commitTime
                      : errors[ run ].getMessage()
                      )
                    );
            
        }
        
    }
    
}
