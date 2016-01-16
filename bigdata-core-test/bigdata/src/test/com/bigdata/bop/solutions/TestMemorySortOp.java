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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
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
                new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
//                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
                new NV(MemorySortOp.Annotations.LAST_PASS, true),//
		}));

        //
        // the test data
        //
    	final IBindingSet data [] = new IBindingSet []
        {
              new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, a } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, e } )
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { c }    )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, a } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, b } )
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, c } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, d } )
            , new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { a }    )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, b } )
        } ;

        //
        // the expected solutions
        //
    	final IBindingSet expected [] = new IBindingSet []
        {
              new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { a }    )
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, e } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, c } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, a } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, d } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, b } )
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { c }    )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, b } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, a } )
        } ;

        final BOpStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

        final UUID queryId = UUID.randomUUID();
        final IQueryContext queryContext = new MockQueryContext(queryId);
		final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
		, null/* indexManager */,queryContext
		);

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                runningQuery, -1/* partitionId */
                , stats, query/* op */, true/* lastInvocation */, source, sink,
                null/* sink2 */
        );
		
//        context.setLastInvocation();

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
		AbstractQueryEngineTestCase.assertSameSolutions(expected, sink.iterator(), ft);

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
        final IConstant<IV> a = new Constant<IV>(new XSDNumericIV(1));
        final IConstant<IV> b = new Constant<IV>(new XSDNumericIV(2));
        final IConstant<IV> c = new Constant<IV>(new XSDNumericIV(3));
        final IConstant<IV> d = new Constant<IV>(new XSDNumericIV(4));
        final IConstant<IV> e = new Constant<IV>(new XSDNumericIV(5));

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
                new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
//                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
                new NV(MemorySortOp.Annotations.LAST_PASS, true),//
        }));

        //
        // the test data
        //
        final IBindingSet data [] = new IBindingSet []
        {
              new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, a } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, e } )
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { c }    )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, a } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, b } )
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, c } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, d } )
            , new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { a }    )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, b } )
        } ;

        //
        // the expected solutions
        //
        final IBindingSet expected [] = new IBindingSet []
        {
              new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { a }    )
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, e } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, c } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { a, a } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, d } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { b, b } )
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { c }    )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, b } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { d, a } )
        } ;

        final BOpStats stats = query.newStats () ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

        final UUID queryId = UUID.randomUUID();
        final IQueryContext queryContext = new MockQueryContext(queryId);
        final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
        , null/* indexManager */,queryContext
        );

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                runningQuery, -1/* partitionId */
                , stats, query/* op */, true/* lastInvocation */, source, sink,
                null/* sink2 */
      );

//        context.setLastInvocation();

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
        AbstractQueryEngineTestCase.assertSameSolutions(expected, sink.iterator(), ft);

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
     * Correct treatment is to treat the type errors as unbound variables
     * see trac-765
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testComputedValueExpressions() 
    {
        final String namespace = getName();
        final String lexiconNamespace;
        final Properties properties = new Properties();
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,BufferMode.MemStore.name());
        final Journal store = new Journal(properties);
        try {
            {
                final AbstractTripleStore kb = new LocalTripleStore(store,
                        namespace, ITx.UNISOLATED, properties);
                kb.create();
                store.commit();
                lexiconNamespace = kb.getLexiconRelation().getNamespace();
            }
        
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> z = Var.var("z");
        final IConstant<IV> _1 = new Constant<IV>(new XSDNumericIV(1));
        final IConstant<IV> _2 = new Constant<IV>(new XSDNumericIV(2));
        final IConstant<IV> _3 = new Constant<IV>(new XSDNumericIV(3));
        final IConstant<IV> _4 = new Constant<IV>(new XSDNumericIV(4));
        final IConstant<IV> _5 = new Constant<IV>(new XSDNumericIV(5));

        final ISortOrder<?> sors[] = new ISortOrder[] { //
                new SortOrder(new Bind(z,new MathBOp(x, y, MathBOp.MathOp.PLUS,new GlobalAnnotations(lexiconNamespace, ITx.READ_COMMITTED))), false/* asc */),//
                new SortOrder(y, false/* asc */), //
                new SortOrder(x, true/* asc */), //
        };

        final int sortOpId = 1;
        
        final SortOp query = new MemorySortOp(new BOp[] {}, NV.asMap(new NV[] {
                new NV(MemorySortOp.Annotations.BOP_ID, sortOpId),
                new NV(MemorySortOp.Annotations.SORT_ORDER,sors),//
                new NV(MemorySortOp.Annotations.VALUE_COMPARATOR, new IVComparator()),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
//                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
                new NV(MemorySortOp.Annotations.LAST_PASS, true),//
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
            , new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { _1 }    )  // x+y=N/A
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _2 } ) // x+y=4
        } ;

        //
        // the expected solutions
        //
        final IBindingSet expected [] = new IBindingSet []
        {
             new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _5 } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _4 } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _2 } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _4, _1 } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _3 } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _2, _2 } )
            , new ListBindingSet ( new IVariable<?> [] { x, y }, new IConstant [] { _1, _1 } )
            , new ListBindingSet ( new IVariable<?> [] { y },    new IConstant [] { _1 }    ) // type error.
            , new ListBindingSet ( new IVariable<?> [] {},       new IConstant [] {}       )  // type error.
            , new ListBindingSet ( new IVariable<?> [] { x },    new IConstant [] { _3 }    ) // type error.
        } ;

        final BOpStats stats = query.newStats () ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                query, stats);

        final UUID queryId = UUID.randomUUID();
        final IQueryContext queryContext = new MockQueryContext(queryId);
        final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
        , store/* indexManager */,queryContext
        );
        
        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                runningQuery, -1/* partitionId */
                , stats, query/* op */, true/* lastInvocation */, source, sink,
                null/* sink2 */
        );

//        context.setLastInvocation();

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
        AbstractQueryEngineTestCase.assertSameSolutions(expected, sink.iterator(), ft);

        assertEquals ( 1, stats.chunksIn.get () ) ;
        assertEquals ( 10, stats.unitsIn.get () ) ;
        assertEquals ( 10, stats.unitsOut.get () ) ;
        assertEquals(1, stats.chunksOut.get());
        } finally {
            store.destroy();
        }
    }

}
