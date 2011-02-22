/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 27, 2010
 */

package com.bigdata.bop.rdf;

import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase2;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.OrBOp;
import com.bigdata.rdf.internal.constraints.ValueExpressionBOp;
import com.bigdata.rdf.store.BD;

/**
 * Unit tests for {@link BOpUtility}.
 */
public class TestBOpUtility extends TestCase2 {

    /**
     * 
     */
    public TestBOpUtility() {
    }

    /**
     * @param name
     */
    public TestBOpUtility(String name) {
        super(name);
    }

    private void eatData(/*final int expectedLength, */final Iterator<?> itr) {
    	int i = 1;
        while (itr.hasNext()) {
            final Object t = itr.next();
//            System.err.print(i+" ");// + " : " + t);
//            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
//                    + t, expected[i].equals(t));
            i++;
        }
//        System.err.println("");
//        assertEquals("#visited", expectedLength, i);
    }

    private BOp generateBOp(final int count,final IValueExpression<?> a) {
    	
    	IValueExpression bop = null;
    	
    	for (int i = 0; i < count; i++) {
    		
        	final IValueExpression c = new DummyVE(
        			new BOp[] { a, new Constant<Integer>(i) }, 
        			null/*annotations*/); 
        	
    		if (bop == null) {
    			bop = c;
    		} else {
    			bop = new OrBOp(c, bop);
    		}
    		
    	}
    	
    	return bop;
    	
    }
    
    /**
     * Unit test for {@link BOpUtility#getSpannedVariables(BOp)}.
     */
    public void test_getSpannedVariables() {

    	final IValueExpression<?> a = Var.var("a");

    	System.err.println("depth, millis");
		final int ntrials = 2000;
		for (int count = 1; count < ntrials; count++) {
			final BOp bop = generateBOp(count, a);
			final long begin = System.currentTimeMillis();
			System.err.print(count);
			eatData(BOpUtility.preOrderIterator(bop));
			final long elapsed = System.currentTimeMillis() - begin;
			System.err.print(", ");
			System.err.print(elapsed);
			System.err.print("\n");
		}

//        System.err.println("preOrderIteratorWithAnnotations");
//        eatData(BOpUtility.preOrderIteratorWithAnnotations(bop));
//
//        System.err.println("getSpannedVariables");
//        eatData(BOpUtility.getSpannedVariables(bop));
//
//        // @todo make the returned set distinct?
//        
//        final Object[] expected = new Object[]{//
//                a,//
//        };
//        // @todo verify the actual data visited.
//		assertSameIterator(expected, BOpUtility.getSpannedVariables(bop));
        
    }
    
    private static class DummyVE extends ValueExpressionBOp {
    	
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1942393209821562541L;

		public DummyVE(BOp[] args, Map<String, Object> annotations) {
			super(args, annotations);
		}

		public DummyVE(ValueExpressionBOp op) {
			super(op);
		}

		public IV get(IBindingSet bindingSet) {
    		throw new RuntimeException();
    	}
    }
    
    public void testOpenWorldEq() throws Exception {
    	
    	final Sail sail = new MemoryStore();
    	final Repository repo = new SailRepository(sail);
    	repo.initialize();
    	final RepositoryConnection cxn = repo.getConnection();
    	
    	try {
    		
    		final ValueFactory vf = sail.getValueFactory();
    		
    		final URI mike = vf.createURI(BD.NAMESPACE + "mike");
    		final URI age = vf.createURI(BD.NAMESPACE + "age");
    		final Literal mikeAge = vf.createLiteral(34);
    		
    		cxn.add(vf.createStatement(mike, RDF.TYPE, RDFS.RESOURCE));
    		cxn.add(vf.createStatement(mike, age, mikeAge));
    		
    		final String query =
    			"select * " +
    			"where { " +
    			"  ?s ?p ?o . " +
    			"  filter (?o < 40) " +
    			"}";
    		
    		final TupleQuery tupleQuery = 
    			cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
    		
    		final TupleQueryResult result = tupleQuery.evaluate();
    		while (result.hasNext()) {
    			System.err.println(result.next());
    		}
    		
    		
    	} finally {
    		cxn.close();
    		repo.shutDown();
    	}
    	
    	
    }

}
