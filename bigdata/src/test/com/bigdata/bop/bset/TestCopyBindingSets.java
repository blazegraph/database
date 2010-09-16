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

package com.bigdata.bop.bset;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Test suite for {@link CopyBindingSetOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCopyBindingSets extends TestCase2 {

    /**
     * 
     */
    public TestCopyBindingSets() {
    }

    /**
     * @param name
     */
    public TestCopyBindingSets(String name) {
        super(name);
    }

    List<IBindingSet> data = null;

    public void setUp() throws Exception {

        setUpData();

    }

    /**
     * Setup the data.
     */
    private void setUpData() {

        final Var<?> x = Var.var("x");

        data = new LinkedList<IBindingSet>();
        IBindingSet bset = null;
        {
            bset = new HashBindingSet();
            bset.set(x, new Constant<String>("John"));
            data.add(bset);
        }
        {
            bset = new HashBindingSet();
            bset.set(x, new Constant<String>("Mary"));
            data.add(bset);
        }
        {
            bset = new HashBindingSet();
            bset.set(x, new Constant<String>("Mary"));
            data.add(bset);
        }
        {
            bset = new HashBindingSet();
            bset.set(x, new Constant<String>("Paul"));
            data.add(bset);
        }
        {
            bset = new HashBindingSet();
            bset.set(x, new Constant<String>("Paul"));
            data.add(bset);
        }
        {
            bset = new HashBindingSet();
            bset.set(x, new Constant<String>("Leon"));
            data.add(bset);
        }

    }

    public void tearDown() throws Exception {

        // clear reference.
        data = null;

    }

    /**
     * Unit test for copying the input to the output.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_copyBindingSets() throws InterruptedException,
            ExecutionException {

        final Var<?> x = Var.var("x");

        final int bopId = 1;

        final CopyBindingSetOp query = new CopyBindingSetOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(BOp.Annotations.BOP_ID, bopId),//
                }));

        // the expected solutions (default sink).
        final IBindingSet[] expected = data.toArray(new IBindingSet[0]);

        final BOpStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data.toArray(new IBindingSet[0]) });

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer(stats);

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, null/* indexManager */),
                -1/* partitionId */, stats, source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);

        // execute task.
        ft.run();

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());

        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

        assertEquals(1L, stats.chunksIn.get());
        assertEquals(6L, stats.unitsIn.get());
        assertEquals(6L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());

    }

}
