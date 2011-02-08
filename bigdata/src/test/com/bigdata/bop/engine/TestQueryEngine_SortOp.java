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
 * Created on Sep 1, 2010
 */

package com.bigdata.bop.engine;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.solutions.ComparatorOp;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.bop.solutions.MemorySortOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Test suite ORDER BY operators when integrated with the query engine. This
 * test suite is designed to examine cases where the ORDER BY operator will have
 * to buffer multiple chunks of solutions before finally placing the buffered
 * solutions into a total order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestQueryEngine2.java 3489 2010-09-01 18:27:35Z thompsonbry $
 * 
 * @todo Test each ORDER BY implementation here (buffered on the Java heap,
 *       buffered on the native heap using the Memory Manager, external memory
 *       sort, radix sort variation, wordsort variation, etc).
 */
public class TestQueryEngine_SortOp extends TestCase2 {

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
     * 
     */
    public TestQueryEngine_SortOp() {
    }

    /**
     * @param name
     */
    public TestQueryEngine_SortOp(String name) {
        super(name);
    }

	public void testStressThreadSafe() throws Exception {

		for (int i = 0; i < 100; i++) {
			
			try {

				test_orderBy_threadSafe();
				
			} catch (Throwable t) {
				
				fail("Failed after " + i + " trials", t);
				
			}
			
		}

	}

	/**
	 * @todo Unit test for ORDER BY. We can judge correctness by ensuring that
	 *       (a) all solutions are visited (either by probing to verify that
	 *       each solution defined and visited or using a counter) and by (b)
	 *       verifing that the solutions are visited in an order for which the
	 *       comparator never returns LT ZERO (0) when comparing with the
	 *       previous solution visited by the query iterator.
	 */
	public void test_orderBy_threadSafe() throws Exception {

		final long timeout = 10000; // ms

		final int ntrials = 10000;

		final int poolSize = 10;

		doOrderByTest(10000/* maxInt */, timeout, ntrials, poolSize);

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
    protected void doOrderByTest(final int maxInt,
            final long timeout, final int ntrials, final int poolSize)
            throws Exception {

    	int ngiven = 0;
    	final IVariable<?> a = Var.var("a");
        final IBindingSet[][] chunks = new IBindingSet[ntrials][];
        {
            final Random r = new Random();
            for (int i = 0; i < chunks.length; i++) {
                // random non-zero chunk size
                chunks[i] = new IBindingSet[r.nextInt(10) + 1];
                for (int j = 0; j < chunks[i].length; j++) {
                    final IBindingSet bset = new ListBindingSet();
					bset.set(a, new Constant<Integer>(r.nextInt(maxInt)));
                    chunks[i][j] = bset;
                    ngiven++;
                }
            }
        }
        
        final int startId = 1;
        final int sortId = 2;

		/*
		 * Note: The StartOp breaks up the initial set of chunks into multiple
		 * IChunkMessages, which results in multiple invocations of the SortOp.
		 */
    	final PipelineOp startOp = new StartOp(new BOp[]{}, NV.asMap(new NV[]{//
                new NV(SliceOp.Annotations.BOP_ID, startId),//
                new NV(MemorySortOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
    	}));

    	final PipelineOp query = new MemorySortOp(new BOp[] {startOp}, NV.asMap(new NV[] {//
                new NV(SliceOp.Annotations.BOP_ID, sortId),//
				new NV(MemorySortOp.Annotations.COMPARATOR,
						new IntegerComparatorOp(
								new ISortOrder[] { new SortOrder(a,
										true) })),//
                new NV(MemorySortOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(MemorySortOp.Annotations.PIPELINED, false),//
        }));

        final UUID queryId = UUID.randomUUID();
        final IRunningQuery q = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId, -1/* partitionId */,
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
        final BOpStats stats = (BOpStats) q.getStats().get(sortId);
		if (log.isInfoEnabled())
			log.info(getClass().getName() + "." + getName() + " : " + stats);
        assertNotNull(stats);
        assertEquals(ngiven, nsolutions);
        assertEquals(ngiven, stats.unitsIn.get());
        assertEquals(ngiven, stats.unitsOut.get());

    }

	/**
	 * Helper class for comparing solution sets having variables which evaluate
	 * to {@link Integer} values.
	 */
    static private class IntegerComparatorOp extends ComparatorOp
    {

    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		/** The sort order. */
    	final private ISortOrder<?> [] _sors;

        public IntegerComparatorOp ( final ISortOrder<?> sors [] )
        {
            super ( new BOp [] {}, NV.asMap ( new NV [] { new NV ( ComparatorOp.Annotations.ORDER, sors ) } ) ) ;
            _sors = sors ;
        }

        public int compare ( IBindingSet o1, IBindingSet o2 )
        {
            for ( ISortOrder<?> sor : _sors )
            {
                int ret = compare ( sor, o1, o2 ) ;
                if ( 0 != ret )
                    return ret ;
            }
            return 0 ;
        }

        private int compare ( ISortOrder<?> sor, IBindingSet lhs, IBindingSet rhs )
        {
            int compare = 0 ;

            IConstant<?> lhsv = lhs.get ( sor.getVariable () ) ;
            IConstant<?> rhsv = rhs.get ( sor.getVariable () ) ;

            if ( null == lhsv && null == rhsv )
                return 0 ;
            else if ( null == lhsv )
                compare = -1 ;
            else if ( null == rhsv )
                compare = 1 ;
            else
				compare = ((Integer) lhsv.get()).compareTo(((Integer) rhsv
						.get())) ;

            return compare * ( sor.isAscending () ? 1 : -1 ) ;
        }
        
    }
    
}
