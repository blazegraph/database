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

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTreeBuilder;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

public class TestRangeBOp extends QuadsTestCase {

    protected static final Logger log = Logger.getLogger(TestRangeBOp.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestRangeBOp() {
    }

    /**
     * @param arg0
     */
    public TestRangeBOp(String arg0) {
        super(arg0);
    }
    
    public void testSimpleRange() throws Exception {
    	
//        final Sail sail = new MemoryStore();
//        sail.initialize();
//        final Repository repo = new SailRepository(sail);

    	final BigdataSail sail = getSail();
    	try {
    	sail.initialize();
    	final BigdataSailRepository repo = new BigdataSailRepository(sail);
    	
    	final RepositoryConnection cxn = repo.getConnection();
        
        try {
            cxn.setAutoCommit(false);
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI mike = vf.createURI(BD.NAMESPACE + "mike");
            final URI bryan = vf.createURI(BD.NAMESPACE + "bryan");
            final URI person = vf.createURI(BD.NAMESPACE + "person");
            final URI age = vf.createURI(BD.NAMESPACE + "age");
            final Literal _1 = vf.createLiteral(1);
            final Literal _2 = vf.createLiteral(2);
            final Literal _3 = vf.createLiteral(3);
            final Literal _4 = vf.createLiteral(4);
            final Literal _5 = vf.createLiteral(5);
            
            /*
             * Create some statements.
             */
            cxn.add(mike, age, _2);
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(bryan, age, _4);
            cxn.add(bryan, RDF.TYPE, person);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            {
            	
	            String query =
	            	QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	            	"prefix rdf: <"+RDF.NAMESPACE+"> " +
	            	"prefix rdfs: <"+RDFS.NAMESPACE+"> " +
	            	
	                "select * " +
	                "where { " +
//	            		"bd:productA bd:property1 ?origProperty1 . " +
//            			"?product bd:property1 ?simProperty1 . " +
//            			"FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
	                "  ?x bd:age ?age . " +
	                "  ?x rdf:type bd:person . " +
	                "  filter(?age > 1 && ?age < 3) " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	           
	            if (log.isInfoEnabled()) {
	            	
	            	log.info(query);
	            	
		            final BigdataSailTupleQuery bdTupleQuery =
		            	(BigdataSailTupleQuery) tupleQuery;
		            final QueryRoot root = (QueryRoot) bdTupleQuery.getTupleExpr();
		            final Projection p = (Projection) root.getArg();
		            final TupleExpr tupleExpr = p.getArg();
		            final SOpTreeBuilder stb = new SOpTreeBuilder();
		            final SOpTree tree = stb.collectSOps(tupleExpr);
	           
//	                log.info(tree);
//	            	log.info(query);

	            	final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	                
	            }
	            
//	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
//	            answer.add(createBindingSet(
//	            		new BindingImpl("a", paul),
//	            		new BindingImpl("b", mary)
//	            		));
//	            answer.add(createBindingSet(
//	            		new BindingImpl("a", brad),
//	            		new BindingImpl("b", john)
//	            		));
//
//	            final TupleQueryResult result = tupleQuery.evaluate();
//                compare(result, answer);

            }
            
        } finally {
            cxn.close();
        }
        } finally {
            sail.__tearDownUnitTest();//shutDown();
        }

    }

}
