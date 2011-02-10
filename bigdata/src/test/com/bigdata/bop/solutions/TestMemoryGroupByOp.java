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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
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
 * Unit tests for {@link MemoryGroupByOp}.
 * 
 * @author thompsonbry
 * 
 * @todo correct rejection tests for various kinds of illegal expressions, such
 *       as having forward references to variables which have not been computed.
 *       There are actually several different ways in which this rule can be
 *       violated, including having forward references within the SELECT (or our
 *       COMPUTE).
 * 
 * @todo correct rejection tests when the SELECT or HAVING clause references a
 *       variable not defined in the aggregated solution groups and not wrapped
 *       by an aggregate function.
 * 
 * @todo test to verify that the evaluation of the aggregate functions within
 *       each group are independent (they have internal state to track the
 *       running value of the aggregate, but that state can not be shared across
 *       groups).
 * 
 * @todo test with various kinds of type errors.
 * 
 * @todo test with DISTINCT used within aggregate functions (this forces us to
 *       consider the distinct solutions within groups for those aggregate
 *       functions which make use of the DISTINCT keyword).
 * 
 * @todo test COUNT(*) and COUNT(DISTINCT *) semantics.
 * 
 * @todo test when some aggregate functions in a GROUP_BY use the DISTINCT
 *       keyword while others in the same GROUP_BY do not.
 * 
 * @todo test with HAVING constraints.
 * 
 * @todo test with multiple invocations of the operator (or do this in the
 *       integration stress test).
 * 
 * @todo Is it possible to test these aggregation operators without testing at
 *       the SPARQL level?
 */
public class TestMemoryGroupByOp extends TestCase2 {

	public TestMemoryGroupByOp() {
	}

	public TestMemoryGroupByOp(String name) {
		super(name);
	}

	public void test_something() {
		fail("write tests");
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
	public void test_simpleGroupBy() {

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
		final IConstant<Integer> price5 = new Constant<Integer>(5);
		final IConstant<Integer> price7 = new Constant<Integer>(7);
		final IConstant<Integer> price9 = new Constant<Integer>(9);
		final IConstant<Integer> price21 = new Constant<Integer>(21);

    	final int groupById = 1;
    	
		final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
				.asMap(new NV[] {//
						new NV(BOp.Annotations.BOP_ID, groupById),//
						new NV(BOp.Annotations.EVALUATION_CONTEXT,
								BOpEvaluationContext.CONTROLLER),//
						new NV(PipelineOp.Annotations.PIPELINED, true),//
						new NV(PipelineOp.Annotations.THREAD_SAFE, false),//
						new NV(GroupByOp.Annotations.SELECT, //
								new IVariable[] { org, lprice }), //
						new NV(GroupByOp.Annotations.COMPUTE,//
								new IValueExpression[] { new SUMInt(
										false/* distinct */,
										(IValueExpression) lprice) }), //
						new NV(GroupByOp.Annotations.GROUP_BY,//
								new IValueExpression[] { org }) //
				}));

        /* the test data:
         * 
		 * ?org  ?auth  ?book  ?lprice
		 * org1  auth1  book1  9
		 * org1  auth1  book3  5
		 * org1  auth2  book3  7
		 * org2  auth3  book4  7
         */
    	final IBindingSet data [] = new IBindingSet []
        {
            new ArrayBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ArrayBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /* the expected solutions:
         * 
         * ?org  ?totalPrice
		 * org1  21
		 * org2   7
         */
    	final IBindingSet expected [] = new IBindingSet []
        {
              new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org1, price21 } )
            , new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org1, price7  } )
        } ;

        final BOpStats stats = query.newStats () ;

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]> ( new IBindingSet [][] { data } ) ;

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(query, stats);

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

		assertEquals(1, stats.chunksIn.get());
		assertEquals(4, stats.unitsIn.get());
		assertEquals(2, stats.unitsOut.get());
		assertEquals(1, stats.chunksOut.get());

	}

	private static class SUMInt extends AggregateBase<Integer> implements IAggregate<Integer> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public SUMInt(BOpBase op) {
			super(op);
		}

		public SUMInt(BOp[] args, Map<String, Object> annotations) {
			super(args, annotations);
		}

		public SUMInt(boolean distinct, IValueExpression<Integer> expr) {
			super(distinct, expr);
		}

		/**
		 * The running aggregate value.
		 * <p>
		 * Note: SUM() returns ZERO if there are no non-error solutions
		 * presented.
		 * <p>
		 * Note: This field is guarded by the monitor on the {@link SUMInt}
		 * instance.
		 */
		private transient int aggregated = 0;
		
		@Override
		synchronized
		public Integer get(final IBindingSet bindingSet) {

			final IValueExpression<Integer> var = (IValueExpression<Integer>) get(0);

			final Integer val = (Integer) var.get(bindingSet);

			if (val != null) {

				// aggregate non-null values.
				aggregated += val;

			}

			return Integer.valueOf(aggregated);

		}

	}

}
