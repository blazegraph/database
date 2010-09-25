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
 * Created on Sep 17, 2010
 */

package com.bigdata.bop.engine;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.EmptyBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SliceOp.SliceStats;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Stress test for {@link SliceOp} in which a large number of small chunks are
 * fed into the query such that the concurrency constraints of the slice are
 * stress tested. {@link SliceOp#isSharedState()} returns <code>true</code> so
 * each invocation of the same {@link SliceOp} operator instance should use the
 * same {@link SliceStats} object. This test will fail if that is not true.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryEngine_Slice extends TestCase2 {

    /**
     * 
     */
    public TestQueryEngine_Slice() {
    }

    /**
     * @param name
     */
    public TestQueryEngine_Slice(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;
        
    }

    Journal jnl;
    QueryEngine queryEngine;
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());
    
        queryEngine = new QueryEngine(jnl);
        
        queryEngine.init();

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
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSetChunks
     *            the chunks of binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSetChunks) {

        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSetChunks);

    }

    public void test_slice_threadSafe() throws Exception {

        final long timeout = 10000; // ms

        final int ntrials = 10000;

        final int poolSize = 10;

        doSliceTest(500L/* offset */, 1500L/* limit */, timeout, ntrials,
                poolSize);
        
    }

    /**
     * 
     * @param timeout
     * @param ntrials
     * @param poolSize
     * 
     * @return The #of successful trials.
     * 
     * @throws Exception
     */
    protected void doSliceTest(final long offset, final long limit,
            final long timeout, final int ntrials, final int poolSize)
            throws Exception {

        final IBindingSet[][] chunks = new IBindingSet[ntrials][];
        {
            final Random r = new Random();
            final IBindingSet bset = EmptyBindingSet.INSTANCE;
            for (int i = 0; i < chunks.length; i++) {
                // random non-zero chunk size
                chunks[i] = new IBindingSet[r.nextInt(10) + 1];
                for (int j = 0; j < chunks[i].length; j++) {
                    chunks[i][j] = bset;
                }
            }
        }
        final int sliceId = 1;
        final SliceOp query = new SliceOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(SliceOp.Annotations.BOP_ID, sliceId),//
                        new NV(SliceOp.Annotations.OFFSET, offset),//
                        new NV(SliceOp.Annotations.LIMIT, limit),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                }));

        final UUID queryId = UUID.randomUUID();
        final RunningQuery q = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        sliceId, -1/* partitionId */,
                        newBindingSetIterator(chunks)));

        // consume solutions.
        int nsolutions = 0;
        final IAsynchronousIterator<IBindingSet[]> itr = q.iterator();
        while (itr.hasNext()) {
            nsolutions += itr.next().length;
        }

        // wait for the query to terminate.
        q.get();

        // Verify stats.
        final SliceStats stats = (SliceStats) q.getStats().get(sliceId);
        System.err.println(getClass().getName() + "." + getName() + " : "
                + stats);
        assertNotNull(stats);
        assertEquals(limit, stats.naccepted.get());
        assertEquals(limit, nsolutions);

    }

}
