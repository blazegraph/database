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
package com.bigdata.bop.engine;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Test suite for {@link QueryDeadline} ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestQueryDeadlineOrder extends TestCase2 {

    public TestQueryDeadlineOrder() {

    }

    public TestQueryDeadlineOrder(final String name) {
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
    private Journal jnl;
    private QueryEngine queryEngine;

    @Override
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
        rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        // Do commit since not scale-out.
        store.commit();

    }

    @Override
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
     * Verify the semantics of {@link QueryDeadline#compareTo(QueryDeadline)}.
     * 
     * @throws Exception
     */
    public void testQueryDeadlineOrder01() throws Exception {

        final long now = System.currentTimeMillis();
        
        final int startId = 1;

        final PipelineOp query1 = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineOp query2 = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final AbstractRunningQuery runningQuery1 = queryEngine.eval(UUID.randomUUID(),
                query1, new ListBindingSet());

        final long deadline1Millis = now + 10000/* millis */;
        
        runningQuery1.setDeadline(deadline1Millis);
        
        Thread.sleep(2);
        
        final AbstractRunningQuery runningQuery2 = queryEngine.eval(UUID.randomUUID(),
                query2, new ListBindingSet());

        final long deadline2Millis = now + 20000/* millis */;
        
        runningQuery2.setDeadline(deadline2Millis);

        final QueryDeadline queryDeadline1 = new QueryDeadline(
                TimeUnit.MILLISECONDS.toNanos(runningQuery1.getDeadline()),
                runningQuery1);

        final QueryDeadline queryDeadline2 = new QueryDeadline(
                TimeUnit.MILLISECONDS.toNanos(runningQuery2.getDeadline()),
                runningQuery2);

        // The earlier deadline is LT the later deadline.
        assertTrue(queryDeadline1.compareTo(queryDeadline2) < 0);

        // The later deadline is GT the earlier deadline.
        assertTrue(queryDeadline2.compareTo(queryDeadline1) > 0);

        // Same deadline.
        assertEquals(0, queryDeadline1.compareTo(queryDeadline1));
        assertEquals(0, queryDeadline2.compareTo(queryDeadline2));

        /*
         * Verify that the query deadline (millis) was converted to nanos for
         * QueryDeadline object.
         */
        assertEquals(TimeUnit.MILLISECONDS.toNanos(deadline1Millis),
                queryDeadline1.deadlineNanos);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(deadline2Millis),
                queryDeadline2.deadlineNanos);
        
    }
    
}
