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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.sop.SOp;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroups;
import com.bigdata.rdf.sail.sop.SOpTreeBuilder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Unit tests the optionals aspects of the {@link BigdataSail} implementation.
 * 
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestNestedUnions extends QuadsTestCase {

	/*
	 * TODO
	 * 
	 * 1. arrange sesame operator tree into bigdata predicates with optional
	 * group specified.
	 * 
	 * 2. re-work how constraints are attached - need to be attached to the
	 * first appearance of a variable within its group, not globally across all
	 * predicates (constraints are group-local)
	 * 
	 * 3. some constraints will be conditional routing op instead of an
	 * actual constraint on a predicate. this occurs when the variables in
	 * the constraint do not appear anywhere in the optional group
	 * 
	 * 4. need to punt the query if we can't evaluate a filter inside an
	 * optional group natively, need to recognize this
	 * 
	 * 5. 
	 */
	
    protected static final Logger log = Logger.getLogger(TestNestedUnions.class);

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
    public TestNestedUnions() {
    }

    /**
     * @param arg0
     */
    public TestNestedUnions(String arg0) {
        super(arg0);
    }
    
    public void testSimplestNestedUnion() throws Exception {
    	
//        final Sail sail = new MemoryStore();
//        sail.initialize();
//        final Repository repo = new SailRepository(sail);

    	final BigdataSail sail = getSail();
    	sail.initialize();
    	final BigdataSailRepository repo = new BigdataSailRepository(sail);
    	
    	final RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI john = vf.createURI(BD.NAMESPACE + "john");
            final URI mary = vf.createURI(BD.NAMESPACE + "mary");
            final URI leon = vf.createURI(BD.NAMESPACE + "leon");
            final URI paul = vf.createURI(BD.NAMESPACE + "paul");
            final URI brad = vf.createURI(BD.NAMESPACE + "brad");
            final URI fred = vf.createURI(BD.NAMESPACE + "fred");
            final URI knows = vf.createURI(BD.NAMESPACE + "knows");

            /*
             * Create some statements.
             */
            cxn.add(paul, knows, mary);
            cxn.add(brad, knows, john);
            cxn.add(mary, knows, fred);
            cxn.add(john, knows, leon);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            {
            	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  { " +
	                "    ?b bd:knows bd:fred . " +
	                "  } UNION { " +
	                "    ?b bd:knows bd:leon . " +
	                "  } " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	           
	            if (log.isInfoEnabled()) {
	            	
		            final BigdataSailTupleQuery bdTupleQuery =
		            	(BigdataSailTupleQuery) tupleQuery;
		            final QueryRoot root = (QueryRoot) bdTupleQuery.getTupleExpr();
		            final Projection p = (Projection) root.getArg();
		            final TupleExpr tupleExpr = p.getArg();
		            final SOpTreeBuilder stb = new SOpTreeBuilder();
		            final SOpTree tree = stb.collectSOps(tupleExpr);
	           
	                log.info(tree);
	            	log.info(query);

	            	final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	                
	            }
	            
	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", mary)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", john)
	            		));

	            final TupleQueryResult result = tupleQuery.evaluate();
                compare(result, answer);

            }
            
        } finally {
            cxn.close();
            sail.shutDown();
        }

    }

}
