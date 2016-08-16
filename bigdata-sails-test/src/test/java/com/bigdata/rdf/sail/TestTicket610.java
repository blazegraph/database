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

import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Model;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * To run this test case, specify the following JVM property:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithoutSids</code>
 *  
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestTicket610 extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestTicket610.class);

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, OwlAxioms.class.getName());
        
        return props;
        
    }

    public TestTicket610() {
    }

    public TestTicket610(String arg0) {
        super(arg0);
    }
    
    public void testBug() throws Exception {
    	
        final URI a = new URIImpl(":a");
        final URI b = new URIImpl(":b");
        
        final Model data = new LinkedHashModel(Arrays.asList(new StatementImpl[] {
        		new StatementImpl(a, RDF.TYPE, OWL.TRANSITIVEPROPERTY),
        		new StatementImpl(b, RDFS.SUBPROPERTYOF, a),
        }));
	  	
        /*
         * The bigdata store, backed by a temporary journal file.
         */
	  	final BigdataSail sail = getSail();
	  	
	  	try {
	  	
	  		sail.initialize();
	  		
  			final BigdataSailRepository bigdataRepo = new BigdataSailRepository(sail);
  			
	  		{ // load the data into the bigdata store
	  			
	  			final RepositoryConnection cxn = bigdataRepo.getConnection();
	  			try {
	  				cxn.setAutoCommit(false);
	  				cxn.add(data);
	  				cxn.commit();
	  			} finally {
	  				cxn.close();
	  			}
	  			
	  		}
	  		
	  		{ // check the closure
	  			
	  			final BigdataSailRepositoryConnection cxn = bigdataRepo.getReadOnlyConnection();
	  			try {
	  				
	  				final AbstractTripleStore store = cxn.getTripleStore();
	  				
	  				if (log.isDebugEnabled()) {
		  				log.info(store.dumpStore(true, true, false));
	  				}
	  				
	  				assertFalse("should not have the (<b> rdf:type owl:TransitiveProperty) inference", store.hasStatement(b, RDF.TYPE, OWL.TRANSITIVEPROPERTY));
	  				
	  			} finally {
	  				cxn.close();
	  			}
	  			
	  		}
          
        } finally {
        	sail.__tearDownUnitTest();
        }
    	
    }

}
