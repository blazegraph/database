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
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for the {@link MemorySortOp}.
 * <p>
 * The test suite for the {@link IVComparator} is responsible for testing the
 * ability to compare inline and non-inline {@link IV}s, placing them into an
 * order which is not inconsistent with the SPARQL ORDER BY semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMemorySortOp extends TestCase2 {

    /**
     * 
     */
    public TestMemorySortOp() {
    }

    /**
     * @param name
     */
    public TestMemorySortOp(String name) {
        super(name);
    }

    private long termId = 1;
    
    private IV<BigdataLiteral, ?> makeIV(final BigdataLiteral lit) {

        final IV<BigdataLiteral, ?> iv = new TermId<BigdataLiteral>(
                VTE.LITERAL, termId++);

        iv.setValue(lit);

        return iv;

    }

    /**
     * Test with materialized IVs.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testMaterializedIVs() 
    {

        final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(getName());
        
    	final IVariable<IV> x = Var.var ( "x" ) ;
    	final IVariable<IV> y = Var.var ( "y" ) ;
        final IConstant<IV> a = new Constant<IV>(makeIV(f.createLiteral("a")));
        final IConstant<IV> b = new Constant<IV>(makeIV(f.createLiteral("b")));
        final IConstant<IV> c = new Constant<IV>(makeIV(f.createLiteral("c")));
        final IConstant<IV> d = new Constant<IV>(makeIV(f.createLiteral("d")));
        final IConstant<IV> e = new Constant<IV>(makeIV(f.createLiteral("e")));

        final ISortOrder<?> sors[] = new ISortOrder[] { //
                new SortOrder(x, true/*asc*/),//
                new SortOrder(y, false/*asc*/)//
                };

    	final int sortOpId = 1;
    	
		final SortOp query = new MemorySortOp(new BOp[] {}, NV.asMap(new NV[] {//
				new NV(MemorySortOp.Annotations.BOP_ID, sortOpId),//
                new NV(MemorySortOp.Annotations.SORT_ORDER,sors),//
                new NV(MemorySortOp.Annotations.VALUE_COMPARATOR, new IVComparator()),//
				new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
						BOpEvaluationContext.CONTROLLER),//
                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
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

        final BOpStats stats = query.newStats (null/*queryContext*/) ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

		final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
		, null/* indexManager */
		);

		final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
				runningQuery, -1/* partitionId */
				, stats, source, sink, null/* sink2 */
		);
		
        context.setLastInvocation();

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

		// Check the solutions.
		TestQueryEngine.assertSameSolutions(expected, sink.iterator(), ft);

        assertEquals ( 1, stats.chunksIn.get () ) ;
        assertEquals ( 10, stats.unitsIn.get () ) ;
        assertEquals ( 10, stats.unitsOut.get () ) ;
        assertEquals ( 1, stats.chunksOut.get () ) ;
    }

    /**
     * Unit test with inline {@link IV}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testInlineIVs() 
    {

        final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(getName());
        
        final IVariable<IV> x = Var.var ( "x" ) ;
        final IVariable<IV> y = Var.var ( "y" ) ;
        final IConstant<IV> a = new Constant<IV>(new XSDIntIV(1));
        final IConstant<IV> b = new Constant<IV>(new XSDIntIV(2));
        final IConstant<IV> c = new Constant<IV>(new XSDIntIV(3));
        final IConstant<IV> d = new Constant<IV>(new XSDIntIV(4));
        final IConstant<IV> e = new Constant<IV>(new XSDIntIV(5));

        final ISortOrder<?> sors[] = new ISortOrder[] { //
                new SortOrder(x, true/*asc*/),//
                new SortOrder(y, false/*asc*/)//
                };

        final int sortOpId = 1;
        
        final SortOp query = new MemorySortOp(new BOp[] {}, NV.asMap(new NV[] {
                new NV(MemorySortOp.Annotations.BOP_ID, sortOpId),
                new NV(MemorySortOp.Annotations.SORT_ORDER,sors),//
                new NV(MemorySortOp.Annotations.VALUE_COMPARATOR, new IVComparator()),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
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

        final BOpStats stats = query.newStats (null/*queryContext*/) ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

        final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
        , null/* indexManager */
        );

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                runningQuery, -1/* partitionId */
                , stats, source, sink, null/* sink2 */
        );

        context.setLastInvocation();

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

        // Check the solutions.
        TestQueryEngine.assertSameSolutions(expected, sink.iterator(), ft);

        assertEquals ( 1, stats.chunksIn.get () ) ;
        assertEquals ( 10, stats.unitsIn.get () ) ;
        assertEquals ( 10, stats.unitsOut.get () ) ;
        assertEquals ( 1, stats.chunksOut.get () ) ;
    }

    /**
     * Test with computed value expressions.
     * <p>
     * Note: Since there are some unbound values for the base variables,
     * solutions in which those variables are not bound will cause type errors.
     * Unless the value expressions are evaluated before we sort the solutions
     * those type errors will propagate out of the sort and fail the query.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testComputedValueExpressions() 
    {

        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> z = Var.var("z");
        final IConstant<IV> _1 = new Constant<IV>(new XSDIntIV(1));
        final IConstant<IV> _2 = new Constant<IV>(new XSDIntIV(2));
        final IConstant<IV> _3 = new Constant<IV>(new XSDIntIV(3));
        final IConstant<IV> _4 = new Constant<IV>(new XSDIntIV(4));
        final IConstant<IV> _5 = new Constant<IV>(new XSDIntIV(5));

        final ISortOrder<?> sors[] = new ISortOrder[] { //
                new SortOrder(new Bind(z,new MathBOp(x, y, MathBOp.MathOp.PLUS)), false/* asc */),//
                new SortOrder(y, false/* asc */) //
        };

        final int sortOpId = 1;
        
        final SortOp query = new MemorySortOp(new BOp[] {}, NV.asMap(new NV[] {
                new NV(MemorySortOp.Annotations.BOP_ID, sortOpId),
                new NV(MemorySortOp.Annotations.SORT_ORDER,sors),//
                new NV(MemorySortOp.Annotations.VALUE_COMPARATOR, new IVComparator()),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
        }));

        //
        // the test data
        //
        final IBindingSet data [] = new IBindingSet []
        {
              new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _1 } ) // x+y=2
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _5 } ) // x+y=6
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { _3 }    )  // x+y=N/A
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _1 } ) // x+y=5
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _2 } ) // x+y=6
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )   // x+y=N/A
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _3 } ) // x+y=4
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _4 } ) // x+y=6
            , new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { _1 }    )  // x+y=1
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _2 } ) // x+y=4
        } ;

        //
        // the expected solutions
        //
        final IBindingSet expected [] = new IBindingSet []
        {
//              new ArrayBindingSet ( new IVariable<?> [] { y },    new IConstant [] { _1 }    ) // dropped by type error.
//            , new ArrayBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )  // dropped by type error.
//            , new ArrayBindingSet ( new IVariable<?> [] { x },    new IConstant [] { _3 }    ) // dropped by type error.
              new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _5 } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _4 } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _2 } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _1 } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _3 } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _2 } )
            , new ArrayBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _1 } )
        } ;

        final BOpStats stats = query.newStats (null/*queryContext*/) ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

        final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
        , null/* indexManager */
        );
        
        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                runningQuery, -1/* partitionId */
                , stats, source, sink, null/* sink2 */
        );

        context.setLastInvocation();

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

        // Check the solutions.
        TestQueryEngine.assertSameSolutions(expected, sink.iterator(), ft);

        assertEquals ( 1, stats.chunksIn.get () ) ;
        assertEquals ( 10, stats.unitsIn.get () ) ;
        assertEquals ( 7, stats.unitsOut.get () ) ;
        assertEquals ( 1, stats.chunksOut.get () ) ;
    }
    
//    ///////////////////////////////////////////////////////////////////
//    ///////////////////////////////////////////////////////////////////
//    ///////////////////////////////////////////////////////////////////
//    @SuppressWarnings("serial")
//    static private class StringComparatorOp extends ComparatorOp
//    {
//    	
//    	/** The sort order. */
//        final private ISortOrder<?> [] _sors;
//
//        public StringComparatorOp ( final ISortOrder<?> sors [] )
//        {
//            super ( new BOp [] {}, NV.asMap ( new NV [] { new NV ( ComparatorOp.Annotations.ORDER, sors ) } ) ) ;
//            _sors = sors ;
//        }
//
//        public int compare(final IBindingSet o1, final IBindingSet o2) {
//            for (ISortOrder<?> sor : _sors) {
//                int ret = compare(sor, o1, o2);
//                if (0 != ret)
//                    return ret;
//            }
//            return 0;
//        }
//
//        private int compare(final ISortOrder<?> sor, final IBindingSet lhs,
//                final IBindingSet rhs) {
//            int compare = 0;
//
//            final Object lhsv = sor.getExpr().get(lhs);
//            final Object rhsv = sor.getExpr().get(rhs);
//
//            if ( null == lhsv && null == rhsv )
//                return 0 ;
//            else if ( null == lhsv )
//                compare = -1 ;
//            else if ( null == rhsv )
//                compare = 1 ;
//            else
//                compare = lhsv.toString ().compareTo ( rhsv.toString () ) ;
//
//            return compare * ( sor.isAscending () ? 1 : -1 ) ;
//        }
//        
//    }

}
