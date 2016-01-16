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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Iterator;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.join.HTreeHashJoinOp;
import com.bigdata.bop.join.JVMHashJoinOp;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Test suite for queries designed to exercise a hash join against an access
 * path.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHashJoin extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestHashJoin() {
    }

    /**
     * @param name
     */
    public TestHashJoin(String name) {
        super(name);
    }

    /**
     * <pre>
     * SELECT ?x ?o
     * WHERE {
     * 
     *   # Turn off the query optimizer for this query so we can control the order
     *   # in which the BGPs will be evaluated.
     *   
     *   hint:Query hint:optimizer "None" .
     * 
     *   # Force the use of the JVM hash joins.
     *   hint:Query hint:nativeHashJoins "false" .
     *   
     *   ?x rdf:type foaf:Person .
     * 
     *   ?x rdfs:label ?o .
     * 
     *   # Request a hash join for the rdfs:label BGP.  
     *   hint:Prior hint:hashJoin "true" .
     * 
     * }
     * </pre>
     * 
     * A simple SELECT query with a query hint which should force the choice of
     * a hash join for one of the predicates.
     */
    public void test_hash_join_1() throws Exception {

        final ASTContainer astContainer = new TestHelper("hash-join-1")
                .runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        if (!BOpUtility.visitAll(queryPlan, JVMHashJoinOp.class).hasNext()) {

            fail("Expecting a JVM-based hash join in the query plan: "
                    + astContainer.toString());
            
        }

    }
    
    /**
     * Variant on {@link #test_hash_join_1()} where we force the use of the
     * {@link HTree}.
     */
    public void test_hash_join_1b() throws Exception {

        final ASTContainer astContainer = new TestHelper("hash-join-1b")
                .runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        if (!BOpUtility.visitAll(queryPlan, HTreeHashJoinOp.class).hasNext()) {

            fail("Expecting an HTree-based hash join in the query plan: "
                    + astContainer.toString());

        }

    }
    
    /**
     * <pre>
     * SELECT ?x ?o
     * WHERE {
     * 
     *   # Turn off the query optimizer for this query so we can control the order
     *   # in which the BGPs will be evaluated.
     *   
     *   hint:Query hint:optimizer "None" .
     * 
     *   ?x rdf:type foaf:Person .
     * 
     *   ?x rdfs:label ?o .
     * 
     *   # Request a hash join for the rdfs:label BGP.  
     *   hint:Prior hint:hashJoin "true" .
     *   hint:Prior hint:com.bigdata.bop.IPredicate.keyOrder "PCSO" . # default is POCS
     * }
     * </pre>
     * 
     * A variant of {@link #test_hash_join_1()} where a hint is used to override
     * the {@link SPOKeyOrder} for the hash join.
     */
    public void test_hash_join_2() throws Exception {

        final ASTContainer astContainer = new TestHelper("hash-join-2")
                .runTest();
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();

        // There are two predicates.
        final Iterator<Predicate> itr = BOpUtility.visitAll(queryPlan,
                Predicate.class);

        /*
         * Find the rdfs:label predicate.  That is the one with the hash join.
         */
        Predicate pred;
        IV p;

        pred = itr.next();
        p = (IV) pred.get(1/* p */).get();

        if (p.getValue().equals(RDF.TYPE)) {
            pred = itr.next();
            p = (IV) pred.get(1/* p */).get();
        }

        assertEquals(RDFS.LABEL, p.getValue());

        // Verify the key order override.
        assertEquals(SPOKeyOrder.PCSO, pred.getKeyOrder());

        // Verify the key order override will be used.
        assertEquals(SPOKeyOrder.PCSO, SPOKeyOrder.getKeyOrder(pred, 4/*keyArity*/));
        
        // Clear the override.
        pred = (Predicate) pred.clearProperty(IPredicate.Annotations.KEY_ORDER);

        assertEquals(null, pred.getKeyOrder());

        // Verify the default key order
        assertEquals(SPOKeyOrder.POCS, SPOKeyOrder.getKeyOrder(pred, 4/*keyArity*/));
        
    }

}
