/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
package com.bigdata.bop.solutions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.bop.rdf.aggregate.SUM;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for {@link MemoryGroupByOp}.
 * 
 * @author thompsonbry
 * 
 * @todo test w/ and w/o DISTINCT within aggregate functions (this forces us to
 *       consider the distinct solutions within groups for those aggregate
 *       functions which make use of the DISTINCT keyword).
 * 
 * TODO test COUNT(*) and COUNT(DISTINCT *) semantics.
 * 
 * TODO test with HAVING constraints.
 * 
 * TODO Unit test w/o GROUP_BY clause (just select with aggregate expressions).
 */
public class TestMemoryGroupByOp extends TestCase2 {
    
	public TestMemoryGroupByOp() {
	}

	public TestMemoryGroupByOp(String name) {
		super(name);
	}

    /**
     * Unit test of SELECT expression which projects a variable projected by a
     * GROUP_BY expression.
     * 
     * <pre>
     * SELECT ?org GROUP BY ?org
     * </pre>
     */
    public void test_groupBy_01() {

        final IVariable<?> org = Var.var("org");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");

        final int groupById = 1;
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { org }), //
                        new NV(GroupByOp.Annotations.GROUP_BY,//
                                new IValueExpression[] { org }) //
                }));

        /**
         * The test data:
         * 
         * <pre>
         * ?org
         * org1
         * org1
         * org1
         * org2
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org1 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org1 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org1 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org2 } )
        };

        /**
         * The expected solutions:
         * 
         * <pre>
         * ?org
         * org1
         * org2
         * </pre>
         */
        final IBindingSet expected [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { org },  new IConstant [] { org1 } )
            , new ArrayBindingSet ( new IVariable<?> [] { org },  new IConstant [] { org2 } )
        } ;

        final BOpStats stats = query.newStats () ;

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
        // Force the solutions to be emitted.
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
        TestQueryEngine.assertSameSolutionsAnyOrder(expected, sink.iterator(),
                ft);

        assertEquals(1, stats.chunksIn.get());
        assertEquals(4, stats.unitsIn.get());
        assertEquals(2, stats.unitsOut.get());
        assertEquals(1, stats.chunksOut.get());

    }

    /**
     * Unit test of SELECT expression which projects a variable projected by a
     * GROUP_BY expression.
     * 
     * <pre>
     * SELECT ?org as ?newVar GROUP BY ?org
     * </pre>
     */
    public void test_groupBy_02() {

        final IVariable<?> org = Var.var("org");
        final IVariable<?> newVar = Var.var("newVar");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");

        final int groupById = 1;
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { new Bind(newVar,org) }), //
                        new NV(GroupByOp.Annotations.GROUP_BY,//
                                new IValueExpression[] { org }) //
                }));

        /**
         * The test data:
         * 
         * <pre>
         * ?org
         * org1
         * org1
         * org1
         * org2
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org1 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org1 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org1 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org }, new IConstant [] { org2 } )
        };

        /**
         * The expected solutions:
         * 
         * <pre>
         * ?newVar
         * org1
         * org2
         * </pre>
         */
        final IBindingSet expected [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { newVar },  new IConstant [] { org1 } )
            , new ArrayBindingSet ( new IVariable<?> [] { newVar },  new IConstant [] { org2 } )
        } ;

        final BOpStats stats = query.newStats () ;

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
        // Force the solutions to be emitted.
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
        TestQueryEngine.assertSameSolutionsAnyOrder(expected, sink.iterator(),
                ft);

        assertEquals(1, stats.chunksIn.get());
        assertEquals(4, stats.unitsIn.get());
        assertEquals(2, stats.unitsOut.get());
        assertEquals(1, stats.chunksOut.get());

    }

    /**
	 * Based on an example in the SPARQL 1.1 Working Draft.
	 * 
	 * <pre>
	 * @prefix : <http://books.example/> .
	 * 
	 * :org1 :affiliates :auth1, :auth2 .
	 * :auth1 :writesBook :book1, :book2 .
	 * :book1 :price 9 .
	 * :book2 :price 5 .
	 * :auth2 :writesBook :book3 .
	 * :book3 :price 7 .
	 * :org2 :affiliates :auth3 .
	 * :auth3 :writesBook :book4 .
	 * :book4 :price 7 .
	 * </pre>
	 * 
	 * <pre>
	 * PREFIX : <http://books.example/>
	 * SELECT ?org, (SUM(?lprice) AS ?totalPrice)
	 * WHERE {
	 *   ?org :affiliates ?auth .
	 *   ?auth :writesBook ?book .
	 *   ?book :price ?lprice .
	 * }
	 * GROUP BY ?org
	 * </pre>
	 * 
	 * The solutions input to the GROUP_BY are:
	 * 
	 * <pre>
	 * ?org  ?auth  ?book  ?lprice
	 * org1  auth1  book1  9
	 * org1  auth1  book3  5
	 * org1  auth2  book3  7
	 * org2  auth3  book4  7
	 * </pre>
	 * 
	 * The aggregated solutions groups are:
	 * 
	 * <pre>
	 * ?org  ?totalPrice
	 * org1  21
	 * org2   7
	 * </pre>
	 * 
	 * @todo Do variant with <code>HAVING (SUM(?lprice) > 10)</code>. The
	 *       solutions are:
	 *       <pre>
	 * ?org ?totalPrice
	 * org1 21
	 * </pre>
	 * 
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public void test_something_simpleGroupBy() {

		final IVariable<?> org = Var.var("org");
		final IVariable<?> auth = Var.var("auth");
		final IVariable<?> book = Var.var("book");
		final IVariable<?> lprice = Var.var("lprice");
		final IVariable<?> totalPrice = Var.var("totalPrice");

		final IConstant<String> org1 = new Constant<String>("org1");
		final IConstant<String> org2 = new Constant<String>("org2");
		final IConstant<String> auth1 = new Constant<String>("auth1");
		final IConstant<String> auth2 = new Constant<String>("auth2");
		final IConstant<String> auth3 = new Constant<String>("auth3");
		final IConstant<String> book1 = new Constant<String>("book1");
		final IConstant<String> book2 = new Constant<String>("book2");
		final IConstant<String> book3 = new Constant<String>("book3");
		final IConstant<String> book4 = new Constant<String>("book4");
        final IConstant<XSDIntIV<BigdataLiteral>> price5 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(5));
        final IConstant<XSDIntIV<BigdataLiteral>> price7 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(7));
        final IConstant<XSDIntIV<BigdataLiteral>> price9 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(9));
        final IConstant<XSDIntIV<BigdataLiteral>> price21 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(21));

    	final int groupById = 1;
    	
        final IValueExpression<?> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

    	final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
				.asMap(new NV[] {//
						new NV(BOp.Annotations.BOP_ID, groupById),//
						new NV(BOp.Annotations.EVALUATION_CONTEXT,
								BOpEvaluationContext.CONTROLLER),//
						new NV(PipelineOp.Annotations.PIPELINED, false),//
						new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { org, totalPriceExpr }), //
                        new NV(GroupByOp.Annotations.GROUP_BY,//
                                new IValueExpression[] { org }) //
                }));

        /**
         * The test data:
         * 
         * <pre>
         * ?org  ?auth  ?book  ?lprice
         * org1  auth1  book1  9
         * org1  auth1  book3  5
         * org1  auth2  book3  7
         * org2  auth3  book4  7
         * </pre>
         * 
         * FIXME These solutions SHOULD NOT need to be mutable. The attempt to
         * bind the aggregate function output on the solution sets is arising
         * from a mix up of the different ways in which we can evaluate the
         * aggregation operation.
         * 
         * 1. There are several optimizations, but the basic approach is to
         * GROUP each incoming solution, appending it to the multiset of
         * solutions for its computed GROUP BY key. Then, for each GROUP and
         * each SELECTed value expression, compute the aggregate function over
         * that group and set it on an output solution. Emit an output solution
         * each time we finish with the value expressions in a GROUP. If the
         * aggregate function uses distinct:=true, e.g., AVG(DISTINCT X), then
         * we have to collect the DISTINCT values of X, where X may be a value
         * expression and not just a simple variable, and compute the aggregate
         * function over those DISTINCT values (value set). Otherwise we compute
         * the aggregate function over all values for the value expression in
         * the group.
         * 
         * 2. One optimizations applies when distinct:=false for all SELECTed
         * value expressions. In this case we can proceed GROUP by GROUP,
         * evaluating each aggregation function incrementally as we visit the
         * each solution in the current group.
         * 
         * 3. Another optimization applies when the aggregation functions can be
         * computed solution by solution. Rather than storing the solution
         * multisets for each group, we can have one "counter" for each
         * aggregate function in an aggregate solution set for a given group.
         * For each solution which is assigned to a given group, we compute the
         * inner value expression for the aggregate function and then present
         * that value to the aggregate function (alternatively, we evaluate the
         * aggregation function within the context of its group-local state).
         * 
         * Do we need a distinct API for each of these optimizations?
         * 
         * Are there aggregation functions which can be optimized by (2) but not
         * by (3)?
         */
    	final IBindingSet data [] = new IBindingSet []
        {
    	    new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /**
         * The expected solutions:
         * 
         * <pre>
         * ?org  ?totalPrice
         * org1  21
         * org2   7
         * </pre>
         */
    	final IBindingSet expected [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org1, price21 } )
            , new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org1, price7  } )
        } ;

        final BOpStats stats = query.newStats () ;

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
		// Force the solutions to be emitted.
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
        TestQueryEngine.assertSameSolutionsAnyOrder(expected, sink.iterator(),
                ft);

		assertEquals(1, stats.chunksIn.get());
		assertEquals(4, stats.unitsIn.get());
		assertEquals(2, stats.unitsOut.get());
		assertEquals(1, stats.chunksOut.get());

	}

}
