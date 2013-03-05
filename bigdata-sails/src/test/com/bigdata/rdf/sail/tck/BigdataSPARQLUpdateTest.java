/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.tck;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.parser.sparql.SPARQLUpdateTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Integration with the openrdf SPARQL 1.1 update test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSPARQLUpdateTest extends SPARQLUpdateTest {

    static private final Logger logger = Logger
            .getLogger(BigdataSPARQLUpdateTest.class);

    public BigdataSPARQLUpdateTest() {
    }

//    public BigdataSparqlUpdateTest(String name) {
//        super(name);
//    }
    
    /**
     * Note: This field MUST be cleared in tearDown or a hard reference will be
     * retained to the backend until the end of CI!
     */
    private IIndexManager backend = null;

    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    public void tearDown()
        throws Exception
    {

        super.tearDown();

        if (backend != null)
            tearDownBackend(backend);

        /*
         * Note: this field MUST be cleared to null or the backing database
         * instance will be held by a hard reference throughout the execution of
         * all unit tests in this test suite!
         */

        backend = null;
    
    }

    protected void tearDownBackend(final IIndexManager backend) {
        
        backend.destroy();
        
    }

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     */
    protected Properties getProperties() {

        final Properties props = new Properties();
        
//        final File journal = BigdataStoreTest.createTempFile();
//        
//        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

        props.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
        // quads mode: quads=true, sids=false, axioms=NoAxioms, vocab=NoVocabulary
        props.setProperty(Options.QUADS_MODE, "true");

        // no justifications
        props.setProperty(Options.JUSTIFY, "false");
        
        // no query time inference
        props.setProperty(Options.QUERY_TIME_EXPANDER, "false");
        
//        // auto-commit only there for TCK
//        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        // exact size only the for TCK
        props.setProperty(Options.EXACT_SIZE, "true");
        
//        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
//        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
        /*
         * disable read/write transactions since this class runs against the
         * unisolated connection.
         */
        props.setProperty(Options.ISOLATABLE_INDICES, "false");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        props.setProperty(Options.TEXT_INDEX, "true");
        
        return props;
        
    }

    @Override
    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }

    @Override
    protected Repository newRepository() throws RepositoryException {

        final Properties props = getProperties();
        
        final BigdataSail sail = new BigdataSail(props);
        
        backend = sail.getDatabase().getIndexManager();

        return new BigdataSailRepository(sail);

//        if (true) {
//            final Properties props = getProperties();
//            
//            if (cannotInlineTests.contains(testURI)){
//                // The test can not be run using XSD inlining.
//                props.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");
//                props.setProperty(Options.INLINE_DATE_TIMES, "false");
//            }
//            
//            if(unicodeStrengthIdentical.contains(testURI)) {
//                // Force identical Unicode comparisons.
//                props.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
//                props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
//            }
//            
//            final BigdataSail sail = new BigdataSail(props);
//            return new DatasetRepository(new BigdataSailRepository(sail));
//        } else {
//            return new DatasetRepository(new SailRepository(new MemoryStore()));
//        }

    }

    /**
     * Note: Overridden to turn off autocommit and commit after the data are
     * loaded.
     */
    @Override
    protected void loadDataset(String datasetFile)
            throws RDFParseException, RepositoryException, IOException
        {
            logger.debug("loading dataset...");
            InputStream dataset = SPARQLUpdateTest.class.getResourceAsStream(datasetFile);
            try {
                con.setAutoCommit(false);
                con.add(dataset, "", RDFFormat.forFileName(datasetFile));//RDFFormat.TRIG);
                con.commit();
            }
            finally {
                dataset.close();
            }
            logger.debug("dataset loaded.");
        }

    /**
	 * Unit test for isolation semantics for a sequences of updates.
	 * 
	 * @throws UpdateExecutionException
	 * @throws MalformedQueryException
	 * @throws RepositoryException
     * @throws QueryEvaluationException 
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/558
	 */
	public void test_ticket538() throws UpdateExecutionException,
			RepositoryException, MalformedQueryException, QueryEvaluationException {

		// the [in] and [out] graphs.
        final URI gin = f.createURI("http://example/in");
        final URI gout = f.createURI("http://example/out");

//		((BigdataSailRepository) con.getRepository()).getDatabase().addTerms(
//				new BigdataValue[] { (BigdataValue) gin, (BigdataValue) gout });

//		// Make sure the target graphs are empty.
//		{
//			final String s = "# Update 1\n"
//					+ "DROP SILENT GRAPH <http://example/in>;\n"
//					+ "DROP SILENT GRAPH <http://example/out> ;\n";
//
//			con.prepareUpdate(QueryLanguage.SPARQL, s).execute();
//		}

		con.prepareUpdate(QueryLanguage.SPARQL, "DROP SILENT ALL").execute();
        // Make sure the graphs are empty.
        assertFalse(con.hasStatement(null,null,null, true, (Resource)gin));
        assertFalse(con.hasStatement(null,null,null, true, (Resource)gout));

		/*
		 * The mutation.
		 * 
		 * A statement is inserted into the [in] graph in one operation. Then
		 * all statements in the [in] graph are copied into the [out] graph.
		 */
		final String s = "# Update 2\n"//
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"//
				+ "INSERT DATA {\n"//
				+ "  GRAPH <http://example/in> {\n"//
				+ "        <http://example/president25> foaf:givenName \"William\" .\n"//
				+ "  }\n"//
				+ "};\n"//
				+ "INSERT {\n"//
				+ "   GRAPH <http://example/out> {\n"//
				+ "       ?s ?p ?v .\n"//
				+ "        }\n"//
				+ "    }\n"//
				+ "WHERE {\n"//
				+ "   GRAPH <http://example/in> {\n"//
				+ "       ?s ?p ?v .\n"//
				+ "        }\n"//
				+ "  }\n"//
				+ ";";

//		// The query - how many statements are in the [out] graph.
//		final String q = "# Query\n"//
//				+ "SELECT (COUNT(*) as ?cnt) {\n"//
//				+ "    GRAPH <http://example/out> {\n"//
//				+ "        ?s ?p ?v .\n"//
//				+ "  }\n"//
//				+ "} LIMIT 10";//

		// run update once.
		con.prepareUpdate(QueryLanguage.SPARQL, s).execute();

		// Both graphs should now have some data.
        assertTrue(con.hasStatement(null,null,null, true, (Resource)gin));

		// Note: Succeeds if you do this a 2nd time.
		if (false)
			con.prepareUpdate(QueryLanguage.SPARQL, s).execute();

		assertTrue(con.hasStatement(null,null,null, true, (Resource)gout));

	}

}
