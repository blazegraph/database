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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.solutions;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for {@link SliceOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSliceOp extends TestCase2 {

    /**
     * 
     */
    public TestSliceOp() {
    }

    /**
     * @param name
     */
    public TestSliceOp(String name) {
        super(name);
    }

//    @Override
//    public Properties getProperties() {
//
//        final Properties p = new Properties(super.getProperties());
//
//        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
//                .toString());
//
//        return p;
//        
//    }
//
//    Journal jnl = null;

    ArrayList<IBindingSet> data;

    public void setUp() throws Exception {

//        jnl = new Journal(getProperties());

        setUpData();

    }

    /**
     * Setup the data.
     */
    private void setUpData() {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        data = new ArrayList<IBindingSet>();
            IBindingSet bset = null;
            { // 0
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("John"));
                bset.set(y, new Constant<String>("Mary"));
                data.add(bset);
            }
            { // 1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
            }
            { // 2
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Jane"));
                data.add(bset);
            }
            { // 3
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("Leon"));
                data.add(bset);
            }
            { // 4
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("John"));
                data.add(bset);
            }
            { // 5
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Leon"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
            }

    }

    public void tearDown() throws Exception {

//        if (jnl != null) {
//            jnl.destroy();
//            jnl = null;
//        }
        
        // clear reference.
        data = null;

    }

    /**
     * Unit test for correct visitation for a variety of offset/limit values.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     * 
     * @todo correct rejection tests for bad offset/limit values.
     */
    public void test_slice() throws InterruptedException, ExecutionException {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final int bopId = 1;
        
        final SliceOp query = new SliceOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(SliceOp.Annotations.BOP_ID, bopId),//
                    new NV(SliceOp.Annotations.OFFSET, 1L),//
                    new NV(SliceOp.Annotations.LIMIT, 3L),//
                }));
        
        // the expected solutions
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Mary"),
                                new Constant<String>("Paul"), }//
                ),
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Mary"),
                                new Constant<String>("Jane") }//
                ),
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Paul"),
                                new Constant<String>("Leon") }//
                ), };

        final BOpStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data.toArray(new IBindingSet[0]) });

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, null/* indexManager */
                ), -1/* partitionId */, stats,
                source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);
        
        // execute task.
//        jnl.getExecutorService().execute(ft);
        ft.run();

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());
        
        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

        assertEquals(1L, stats.chunksIn.get());
        assertEquals(4L, stats.unitsIn.get());
        assertEquals(3L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());

    }

}
