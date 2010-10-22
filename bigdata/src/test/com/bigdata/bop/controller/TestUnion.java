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
 * Created on Sep 2, 2010
 */

package com.bigdata.bop.controller;


import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.EmptyBindingSet;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Dechunkerator;

/**
 * Test suite for {@link Union}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestUnionBindingSets.java 3500 2010-09-03 00:27:45Z thompsonbry
 *          $
 */
public class TestUnion extends TestCase2 {

    /**
     * 
     */
    public TestUnion() {
    }

    /**
     * @param name
     */
    public TestUnion(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;

    }

    static private final String namespace = "ns";

    Journal jnl;

    QueryEngine queryEngine;

    public void setUp() throws Exception {

        jnl = new Journal(getProperties());

        loadData(jnl);

        queryEngine = new QueryEngine(jnl);

        queryEngine.init();

    }

    /**
     * Create and populate relation in the {@link #namespace}.
     */
    private void loadData(final Journal store) {

        // create the relation.
        final R rel = new R(store, namespace, ITx.UNISOLATED, new Properties());
        rel.create();

        // data to insert (in key order for convenience).
        final E[] a = {//
        new E("John", "Mary"),// [0]
                new E("Leon", "Paul"),// [1]
                new E("Mary", "Paul"),// [2]
                new E("Paul", "Leon"),// [3]
        };

        // insert data (the records are not pre-sorted).
        rel
                .insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        // Do commit since not scale-out.
        store.commit();

    }

    public void tearDown() throws Exception {

        if (queryEngine != null) {
            queryEngine.shutdownNow();
            queryEngine = null;
        }

        if (jnl != null) {
            jnl.destroy();
            jnl = null;
        }

    }

    /**
     * Verifies that the UNION of two operators is computed. The operators do
     * not route around the UNION, so their solutions are copied to the UNION
     * and from the UNION to onto the top-level query buffer. For this test
     * variant, both subqueries run using their default inputs, which is a
     * single empty binding set each. This gives us two empty binding sets as
     * the output of the union.
     * 
     * @throws Exception
     */
    public void test_union_defaultInputs() throws Exception {

        final int startId1 = 1;
        final int startId2 = 2;
        final int unionId = 3;

        final BOp startOp1 = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(StartOp.Annotations.BOP_ID, startId1),//
                new NV(StartOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final BOp startOp2 = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(StartOp.Annotations.BOP_ID, startId2),//
                new NV(StartOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final BOp unionOp = new Union(new BOp[] { startOp1, startOp2 }, NV
                .asMap(new NV[] {//
                        new NV(Union.Annotations.BOP_ID, unionId),//
                        new NV(Union.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(Union.Annotations.CONTROLLER, true),//
                }));
        
        final BOp query = unionOp;

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                EmptyBindingSet.INSTANCE,
                EmptyBindingSet.INSTANCE,
        };

        final RunningQuery runningQuery = queryEngine.eval(query);

        // verify solutions.
        TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                new Dechunkerator<IBindingSet>(runningQuery.iterator()));

        // Wait until the query is done.
        runningQuery.get();

    }

    /**
     * Verifies that the UNION of two operators is computed. The operators do
     * not route around the UNION, so their solutions are copied to the UNION
     * and from the UNION to a slice.
     * 
     * @throws Exception 
     */
    public void test_union() throws Exception {

        final int startId1 = 1;
        final int startId2 = 2;
        final int unionId = 3;
        final int sliceId = 4;

        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IBindingSet[] bindingSets1 = new IBindingSet[1];
        {
            final IBindingSet tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Leon"));
            bindingSets1[0] = tmp;
        }
        
        final IBindingSet[] bindingSets2 = new IBindingSet[1];
        {
            final IBindingSet tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Mary"));
            tmp.set(y, new Constant<String>("John"));
            bindingSets2[0] = tmp;
        }
        
        final BOp startOp1 = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(StartOp.Annotations.BOP_ID, startId1),//
                new NV(StartOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(StartOp.Annotations.BINDING_SETS,bindingSets1)
                }));

        final BOp startOp2 = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(StartOp.Annotations.BOP_ID, startId2),//
                new NV(StartOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(StartOp.Annotations.BINDING_SETS,bindingSets2)
                }));

        final BOp unionOp = new Union(new BOp[] { startOp1, startOp2 }, NV
                .asMap(new NV[] {//
                        new NV(Union.Annotations.BOP_ID, unionId),//
                        new NV(Union.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(Union.Annotations.CONTROLLER, true),//
                }));

        final BOp sliceOp = new SliceOp(new BOp[]{unionOp},NV.asMap(
                new NV(Union.Annotations.BOP_ID, sliceId),//
                new NV(Union.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER)//
                ));
        
        final BOp query = sliceOp;

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
            new IVariable[] { x },//
            new IConstant[] { new Constant<String>("Leon") }//
            ), //
        new ArrayBindingSet(//
            new IVariable[] { x, y },//
            new IConstant[] { new Constant<String>("Mary"), 
                new Constant<String>("John") }//
        ),//
        };

        final RunningQuery runningQuery = queryEngine.eval(query);

        // verify solutions.
        TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                new Dechunkerator<IBindingSet>(runningQuery.iterator()));

        // Wait until the query is done.
        runningQuery.get();

    }

    /**
     * Verifies that the UNION of two operators is computed. The operators route
     * around the UNION to its parent, which is a SLICE.
     * 
     * @throws Exception
     */
    public void test_union_routeAround() throws Exception {

        final int startId1 = 1;
        final int startId2 = 2;
        final int unionId = 3;
        final int sliceId = 4;

        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IBindingSet[] bindingSets1 = new IBindingSet[1];
        {
            final IBindingSet tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Leon"));
            bindingSets1[0] = tmp;
        }
        
        final IBindingSet[] bindingSets2 = new IBindingSet[1];
        {
            final IBindingSet tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Mary"));
            tmp.set(y, new Constant<String>("John"));
            bindingSets2[0] = tmp;
        }
        
        final BOp startOp1 = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(StartOp.Annotations.BOP_ID, startId1),//
                new NV(StartOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(StartOp.Annotations.SINK_REF, sliceId),//
                new NV(StartOp.Annotations.BINDING_SETS,bindingSets1)
                }));

        final BOp startOp2 = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(StartOp.Annotations.BOP_ID, startId2),//
                new NV(StartOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                    new NV(StartOp.Annotations.SINK_REF, sliceId),//
                new NV(StartOp.Annotations.BINDING_SETS,bindingSets2)
                }));

        final BOp unionOp = new Union(new BOp[] { startOp1, startOp2 }, NV
                .asMap(new NV[] {//
                        new NV(Union.Annotations.BOP_ID, unionId),//
                        new NV(Union.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(Union.Annotations.CONTROLLER, true),//
                }));

        final BOp sliceOp = new SliceOp(new BOp[]{unionOp},NV.asMap(
                new NV(Union.Annotations.BOP_ID, sliceId),//
                new NV(Union.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER)//
                ));
        
        final BOp query = sliceOp;

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
            new IVariable[] { x },//
            new IConstant[] { new Constant<String>("Leon") }//
            ), //
        new ArrayBindingSet(//
            new IVariable[] { x, y },//
            new IConstant[] { new Constant<String>("Mary"), 
                new Constant<String>("John") }//
        ),//
        };

        final RunningQuery runningQuery = queryEngine.eval(query);

        // verify solutions.
        TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                new Dechunkerator<IBindingSet>(runningQuery.iterator()));

        // Wait until the query is done.
        runningQuery.get();

    }

}
