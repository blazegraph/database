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

public class TestSesameFilters extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestSesameFilters.class);

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
    public TestSesameFilters() {
    }

    /**
     * @param arg0
     */
    public TestSesameFilters(String arg0) {
        super(arg0);
    }
    
    public void testRegex() throws Exception {
    	
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
            final URI mike = vf.createURI(BD.NAMESPACE + "mike");
            final URI bryan = vf.createURI(BD.NAMESPACE + "bryan");
            final URI person = vf.createURI(BD.NAMESPACE + "Person");
            final Literal l1 = vf.createLiteral("mike personick");
            final Literal l2 = vf.createLiteral("bryan thompson");

            /*
             * Create some statements.
             */
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, RDFS.LABEL, l1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, RDFS.LABEL, l2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            {
            	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	            	"prefix rdf: <"+RDF.NAMESPACE+"> " +
	            	"prefix rdfs: <"+RDFS.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?s rdf:type bd:Person . " +
	                "  ?s rdfs:label ?label . " +
	                "  FILTER regex(?label, \"mike\") . " +
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
	            		new BindingImpl("s", mike),
	            		new BindingImpl("label", l1)
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
