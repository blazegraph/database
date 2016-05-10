/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.BDS;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Unit test template for use in submission of bugs.
 * <p>
 * This test case will delegate to an underlying backing store.  You can
 * specify this store via a JVM property as follows:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 * <p>
 * There are three possible configurations for the testClass:
 * <ul>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithQuads (quads mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithoutSids (triples mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithSids (SIDs mode)</li>
 * </ul>
 * <p>
 * The default for triples and SIDs mode is for inference with truth maintenance
 * to be on.  If you would like to turn off inference, make sure to do so in
 * {@link #getProperties()}.
 *  
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestTicket581 extends QuadsTestCase {

    protected static final Logger log = Logger.getLogger(TestTicket581.class);

    /**
     * Please set your database properties here, except for your journal file,
     * please DO NOT SPECIFY A JOURNAL FILE. 
     */
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        /*
         * For example, here is a set of five properties that turns off
         * inference, truth maintenance, and the free text index.
         */
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "true");
        
        return props;
        
    }

    public TestTicket581() {
    }

    public TestTicket581(String arg0) {
        super(arg0);
    }
    
    public void testBug() throws Exception {
    	
        /*
         * The bigdata store, backed by a temporary journal file.
         */
	  	final BigdataSail bigdataSail = getSail();
	  	
	  	/*
	  	 * Data file containing the data demonstrating your bug.
	  	 */
	  	final String data = "fulltextsearchwithsubselect.ttl";
	  	final String baseURI = "";
	  	final RDFFormat format = RDFFormat.TURTLE;
	  	
	  	/*
	  	 * Query(ies) demonstrating your bug.
	  	 */
        final String query =
        	"CONSTRUCT { ?object ?p ?o . } " +
//            "WITH { " +
//			"		SELECT DISTINCT ?object WHERE { " +
//			"			?object ?sp ?so . ?so <"+BD.SEARCH+"> \"music\" . " +
//			"		} " +
//			"} as %set1 " +
			"WHERE { " +
//			"	?so <"+BD.SEARCH+"> \"music\" . " +
			"   service <"+BDS.SEARCH+"> { " +
			"	  ?so <"+BDS.SEARCH+"> \"music\" . " +
            "   } " +
			"	?object ?p ?so . " +
			"	?object ?p ?o . " +
			"}"; 
//			"	{ " +
//			"		SELECT DISTINCT ?object WHERE { " +
//			"			?object ?sp ?so . ?so <"+BD.SEARCH+"> \"music\" . " +
//			"		} " +
//			"	} " +
////			"   include %set1 . " +
//			"	OPTIONAL { ?object ?p ?o . } " + 
//			"} ORDER BY ?object ?p"
//			;
	  	
	  	try {
	  	
	  		bigdataSail.initialize();
	  		
  			final BigdataSailRepository bigdataRepo = new BigdataSailRepository(bigdataSail);
  			
	  		{ // load the data into the bigdata store
	  			
	  			final RepositoryConnection cxn = bigdataRepo.getConnection();
	  			try {
	  				cxn.setAutoCommit(false);
	  				cxn.add(getClass().getResourceAsStream(data), baseURI, format);
	  				cxn.commit();
	  			} finally {
	  				cxn.close();
	  			}
	  			
	  		}
	  		
            /*
             * Run the problem query using the bigdata store and then compare
             * the answer.
             */
            final RepositoryConnection cxn = bigdataRepo.getReadOnlyConnection();
  			try {
	            final SailGraphQuery graphQuery = (SailGraphQuery)
	                cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
            	
	            if (log.isInfoEnabled()) {
		            final GraphQueryResult result = graphQuery.evaluate();
            		log.info("bigdata results:");
            		if (!result.hasNext()) {
            			log.info("no results.");
            		}
	                while (result.hasNext()) {
	            		log.info(result.next());
		            }
	            }
	            
  			} finally {
  				cxn.close();
  			}
          
        } finally {
        	bigdataSail.__tearDownUnitTest();
        }
    	
    }

}
