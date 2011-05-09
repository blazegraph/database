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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test case for reverse lookup from SID to statement.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestSids extends ProxyBigdataSailTestCase {

    private static final transient Logger log = Logger.getLogger(TestSids.class);
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestSids() {
    }

    /**
     * @param arg0
     */
    public TestSids(String arg0) {
        super(arg0);
    }

    public void testSids() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            cxn.add(getClass().getResourceAsStream("sids.rdf"), "", RDFFormat.RDFXML);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
            	final String s = null;
//            	final String s = "http://localhost/host1";
//            	final String s = "http://localhost/switch1";
            	
                String query = 
                    "PREFIX myns: <http://mynamespace.com#> " +
                    "SELECT distinct ?s ?p ?o " +
                    " { " +
                    "   ?sid myns:creator <http://1.com> . " +
//                    "   graph ?sid { ?s ?p ?o } " +
                    "   graph ?sid { "+(s == null ? "?s" : "<"+s+">")+" ?p ?o } " +
                    " }";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                
                {
	                final TupleQueryResult result = tupleQuery.evaluate();
	
	                if (!result.hasNext()) {
	                	log.info("no bindings");
	                } else {
		                while (result.hasNext()) {
		                    BindingSet bs = result.next();
	//	                    log.info(bs.getBinding("s").getValue() + " " + bs.getBinding("p").getValue() + " " + bs.getBinding("o").getValue() + " .");
		                    log.info((s == null ? bs.getBinding("s").getValue() : s) + " " + bs.getBinding("p").getValue() + " " + bs.getBinding("o").getValue() + " .");
		                }
	                }
                }
                
                {
                
	                final TupleQueryResult result = tupleQuery.evaluate();
	            	
	                Collection<BindingSet> solution = new LinkedList<BindingSet>();
	                solution.add(createBindingSet(new Binding[] {
	                        new BindingImpl("s", new URIImpl("http://localhost/host1")),
	                        new BindingImpl("p", new URIImpl("http://mynamespace.com#connectedTo")),
	                        new BindingImpl("o", new URIImpl("http://localhost/switch1")),
	                }));
	                solution.add(createBindingSet(new Binding[] {
	                        new BindingImpl("s", new URIImpl("http://localhost/host1")),
	                        new BindingImpl("p", RDF.TYPE),
	                        new BindingImpl("o", new URIImpl("http://domainnamespace.com/host#Host")),
	                }));
	                solution.add(createBindingSet(new Binding[] {
	                        new BindingImpl("s", new URIImpl("http://localhost/switch1")),
	                        new BindingImpl("p", RDF.TYPE),
	                        new BindingImpl("o", new URIImpl("http://domainnamespace.com/san#Switch")),
	                }));
                
	                compare(result, solution);
	                
                }
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testSids2() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
        	final ValueFactory vf = sail.getValueFactory();
        	
        	final URI host1 = vf.createURI("http://localhost/host1");
        	final URI host = vf.createURI("http://domainnamespace.com/host#Host");
        	final URI swtch1 = vf.createURI("http://localhost/switch1");
        	final URI swtch2 = vf.createURI("http://localhost/switch2");
        	final URI swtch = vf.createURI("http://domainnamespace.com/san#Switch");
        	final URI connectedTo = vf.createURI("http://mynamespace.com#connectedTo");
        	final URI creator = vf.createURI("http://mynamespace.com#creator");
        	final URI src1 = vf.createURI("http://1.com");
        	final URI src2 = vf.createURI("http://2.com");
        	final BNode sid1 = vf.createBNode("s1");
        	final BNode sid2 = vf.createBNode("s2");
        	final BNode sid3 = vf.createBNode("s3");
        	final BNode sid4 = vf.createBNode("s4");
        	final BNode sid5 = vf.createBNode("s5");
        	
        	cxn.add(host1, RDF.TYPE, host, sid1);
        	cxn.add(sid1, creator, src1);
        	cxn.add(sid1, creator, src2);
        	
        	cxn.add(host1, connectedTo, swtch1, sid2);
        	cxn.add(sid2, creator, src1);

        	cxn.add(host1, connectedTo, swtch2, sid3);
        	cxn.add(sid3, creator, src2);

        	cxn.add(swtch1, RDF.TYPE, swtch, sid4);
        	cxn.add(sid4, creator, src1);

        	cxn.add(swtch2, RDF.TYPE, swtch, sid5);
        	cxn.add(sid5, creator, src2);

        	cxn.flush();//commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
            	final String s = null;
//            	final String s = "http://localhost/host1";
//            	final String s = "http://localhost/switch1";
            	
                String query = 
                    "PREFIX myns: <http://mynamespace.com#> " +
                    "SELECT distinct ?s ?p ?o " +
                    " { " +
                    "   ?sid myns:creator <http://1.com> . " +
//                    "   graph ?sid { ?s ?p ?o } " +
                    "   graph ?sid { "+(s == null ? "?s" : "<"+s+">")+" ?p ?o } " +
                    " }";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                
                {
	                final TupleQueryResult result = tupleQuery.evaluate();
	
	                if (!result.hasNext()) {
	                	log.info("no bindings");
	                } else {
		                while (result.hasNext()) {
		                    BindingSet bs = result.next();
	//	                    log.info(bs.getBinding("s").getValue() + " " + bs.getBinding("p").getValue() + " " + bs.getBinding("o").getValue() + " .");
		                    log.info((s == null ? bs.getBinding("s").getValue() : s) + " " + bs.getBinding("p").getValue() + " " + bs.getBinding("o").getValue() + " .");
		                }
	                }
                }
                
                {
                
	                final TupleQueryResult result = tupleQuery.evaluate();
	            	
	                Collection<BindingSet> solution = new LinkedList<BindingSet>();
	                solution.add(createBindingSet(new Binding[] {
	                        new BindingImpl("s", new URIImpl("http://localhost/host1")),
	                        new BindingImpl("p", new URIImpl("http://mynamespace.com#connectedTo")),
	                        new BindingImpl("o", new URIImpl("http://localhost/switch1")),
	                }));
	                solution.add(createBindingSet(new Binding[] {
	                        new BindingImpl("s", new URIImpl("http://localhost/host1")),
	                        new BindingImpl("p", RDF.TYPE),
	                        new BindingImpl("o", new URIImpl("http://domainnamespace.com/host#Host")),
	                }));
	                solution.add(createBindingSet(new Binding[] {
	                        new BindingImpl("s", new URIImpl("http://localhost/switch1")),
	                        new BindingImpl("p", RDF.TYPE),
	                        new BindingImpl("o", new URIImpl("http://domainnamespace.com/san#Switch")),
	                }));
                
	                compare(result, solution);
	                
                }
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    
}
