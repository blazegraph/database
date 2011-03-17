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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for the {@link MemorySortOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME This needs to test for computed expressions as well.
 */
public class TestMemorySortOp extends TestCase2 {

    /**
     * 
     */
    public TestMemorySortOp () {}

    /**
     * @param name
     */
    public TestMemorySortOp ( String name )
    {
        super ( name ) ;
    }

    public void testEval () 
    {
    	final IVariable<?> x = Var.var ( "x" ) ;
    	final IVariable<?> y = Var.var ( "y" ) ;
    	final IConstant<String> a = new Constant<String> ( "a" ) ;
    	final IConstant<String> b = new Constant<String> ( "b" ) ;
    	final IConstant<String> c = new Constant<String> ( "c" ) ;
    	final IConstant<String> d = new Constant<String> ( "d" ) ;
    	final IConstant<String> e = new Constant<String> ( "e" ) ;

    	final ISortOrder<?> sors [] = new ISortOrder [] { new SortOrder ( x, true ), new SortOrder ( y, false ) } ;

    	final int sortOpId = 1;
    	
		final SortOp query = new MemorySortOp(new BOp[] {}, NV.asMap(new NV[] {
				new NV(MemorySortOp.Annotations.BOP_ID, sortOpId),
				new NV(MemorySortOp.Annotations.COMPARATOR,
						new StringComparatorOp(sors)),
				new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
						BOpEvaluationContext.CONTROLLER),//
				new NV(MemorySortOp.Annotations.PIPELINED, false),//
				new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
		}));

        //
        // the test data
        //
    	final IBindingSet data [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, a } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, e } )
            , new ArrayBindingSet ( new IVariable<?> [] { x },    new IConstant [] { c }    )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, a } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, b } )
            , new ArrayBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, c } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, d } )
            , new ArrayBindingSet ( new IVariable<?> [] { y },    new IConstant [] { a }    )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, b } )
        } ;

        //
        // the expected solutions
        //
    	final IBindingSet expected [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { y },    new IConstant [] { a }    )
            , new ArrayBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, e } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, c } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, a } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, d } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, b } )
            , new ArrayBindingSet ( new IVariable<?> [] { x },    new IConstant [] { c }    )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, b } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, a } )
        } ;

        final BOpStats stats = query.newStats () ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]> ( new IBindingSet [][] { data } ) ;

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(query, stats);

		final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
		, null/* indexManager */
		);
//		{
//			/**
//			 * Override method not supported by the MockRunningQuery to return
//			 * true, indicating that no more solutions will be presented to the
//			 * MemorySortOp.
//			 */
//			@Override
//			public boolean isLastInvocation(final int bopId,final int nconsumed) {
//				if (bopId == sortOpId) {
//					return true;
//				}
//				return super.isLastInvocation(bopId,nconsumed);
//			}
//		};
		final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
				runningQuery, -1/* partitionId */
				, stats, source, sink, null/* sink2 */
		);

		final FutureTask<Void> ft = query.eval(context);
		// Run the query.
		{
			final Thread t = new Thread() {
				public void run() {
					ft.run();
				}
			};
			t.setDaemon(true);
			t.start();
		}

		try {
			// Check the solutions.
			TestQueryEngine.assertSameSolutions(expected, sink.iterator());
		} finally {
			/* Always wait for the future afterwards and test it for errors. */
			try {
				ft.get();
			} catch (Throwable ex) {
				log.error("Evaluation failed: " + ex, ex);
			}
		}

        assertEquals ( 1, stats.chunksIn.get () ) ;
        assertEquals ( 10, stats.unitsIn.get () ) ;
        assertEquals ( 10, stats.unitsOut.get () ) ;
        assertEquals ( 1, stats.chunksOut.get () ) ;
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////
    @SuppressWarnings("serial")
    static private class StringComparatorOp extends ComparatorOp
    {
    	
    	/** The sort order. */
        final private ISortOrder<?> [] _sors;

        public StringComparatorOp ( final ISortOrder<?> sors [] )
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

            final IConstant<?> lhsv = lhs.get ( sor.getVariable () ) ;
            final IConstant<?> rhsv = rhs.get ( sor.getVariable () ) ;

            if ( null == lhsv && null == rhsv )
                return 0 ;
            else if ( null == lhsv )
                compare = -1 ;
            else if ( null == rhsv )
                compare = 1 ;
            else
                compare = lhsv.toString ().compareTo ( rhsv.toString () ) ;

            return compare * ( sor.isAscending () ? 1 : -1 ) ;
        }
        
    }

}
