/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

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
/*
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for ticket #1086: when loading quads data into a triples store,
 * there now is a config option BigdataSail.Options.REJECT_QUADS_IN_TRIPLE_MODE.
 * When this option is not set, the quads shall be simply loaded will stripping
 * the context away; otherwise, an exception shall be thrown. This test case
 * tests both situations.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestTicket1086 extends ProxyBigdataSailTestCase {

    public Properties getTriplesNoInference() {
        
        Properties props = super.getProperties();
        
        // triples with sids
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
        
        // no inference
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    
    /**
     * Returns a configuration where stripping of quads within the loading
     * process is disabled.
     * @return
     */
    public Properties getTriplesNoInferenceNoQuadsStripping() {
       
       Properties props = getTriplesNoInference();
       props.setProperty(BigdataSail.Options.REJECT_QUADS_IN_TRIPLE_MODE, "true");
       
       return props;       
   }    
    

    /**
     * 
     */
    public TestTicket1086() {
    }

    /**
     * @param arg0
     */
    public TestTicket1086(String arg0) {
        super(arg0);
    }

    
    /**
     * When loading quads into a triple store, the context is striped away by
     * default.
     */
    public void testQuadStripping() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getTriplesNoInference());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

            final BigdataValueFactory vf = (BigdataValueFactory) sail
                  .getValueFactory();
            final URI s = vf.createURI("http://test/s");
            final URI p = vf.createURI("http://test/p");
            final URI o = vf.createURI("http://test/o");
            final URI c = vf.createURI("http://test/c");

            BigdataStatement stmt = vf.createStatement(s, p, o, c);
            cxn.add(stmt); 

            RepositoryResult<Statement> stmts = cxn.getStatements(null, null,
                  null, false);
            Statement res = stmts.next();
            assertEquals(s, res.getSubject());
            assertEquals(p, res.getPredicate());
            assertEquals(o, res.getObject());
            assertEquals(null, res.getContext());

        } finally {
            if (cxn != null)
                cxn.close();
            sail.__tearDownUnitTest();
        }
    }

   /**
    * When loading quads into a triple store and the BigdataSail option
    * REJECT_QUADS_IN_TRIPLE_MODE is set to true, an exception will be thrown.
    */
   public void testQuadStrippingRejected() throws Exception {

      BigdataSailRepositoryConnection cxn = null;

      final BigdataSail sail = getSail(getTriplesNoInferenceNoQuadsStripping());

      boolean exceptionEncountered = false;
      try {

         sail.initialize();
         final BigdataSailRepository repo = new BigdataSailRepository(sail);
         cxn = (BigdataSailRepositoryConnection) repo.getConnection();

         final BigdataValueFactory vf = (BigdataValueFactory) sail
               .getValueFactory();
         final URI s = vf.createURI("http://test/s");
         final URI p = vf.createURI("http://test/p");
         final URI o = vf.createURI("http://test/o");
         final URI c = vf.createURI("http://test/c");

         BigdataStatement stmt = vf.createStatement(s, p, o, c);
         cxn.add(stmt);

      } catch (RepositoryException e) {
         
         exceptionEncountered = true; // expected !
         
      } finally {
         
         if (cxn != null)
            cxn.close();
         sail.__tearDownUnitTest();
      }
      
      assertTrue(exceptionEncountered);
   }    
}
