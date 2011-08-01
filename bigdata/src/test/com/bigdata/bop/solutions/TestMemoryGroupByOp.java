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

import java.math.BigInteger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableFactory;
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
import com.bigdata.bop.rdf.aggregate.COUNT;
import com.bigdata.bop.rdf.aggregate.SUM;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSDBooleanIV;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.XSDIntegerIV;
import com.bigdata.rdf.internal.XSDLongIV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for {@link MemoryGroupByOp}.
 * 
 * @author thompsonbry
 */
public class TestMemoryGroupByOp extends TestCase2 {
    
	public TestMemoryGroupByOp() {
	}

	public TestMemoryGroupByOp(String name) {
		super(name);
	}

    /**
     * Provides sequential, predictable, and easily read variable names.
     */
    private static class MockVariableFactory implements IVariableFactory {

        int i = 0;

        public IVariable<?> var() {

            return Var.var("_" + i++);
            
        }

    }

    /**
     * Unit test of SELECT expression which projects a variable projected by a
     * GROUP_BY expression.
     * 
     * <pre>
     * SELECT ?org GROUP BY ?org
     * </pre>
     */
    public void test_select_projects_group_by_variable() {

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
    public void test_select_projects_group_by_value_expression_as_variable() {

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
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public void test_simple_groupBy_with_aggregate() {

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

    	final int groupById = 1;
    	
        final IValueExpression<?> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

        final IVariableFactory variableFactory = new MockVariableFactory();
        
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
                })) {
    	    @Override
    	    public IVariable<?> var() {
    	        return variableFactory.var();
    	    }

    	};

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
        
    	// Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price7 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(7)));
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price21 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(21)));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org1, _price21 } )
            , new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org2, _price7  } )
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
     * HAVING (SUM(?lprice) > 10)
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
     * org1 21
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_groupBy_with_aggregate_and_having() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

        /* FIXME Should all be IVs based on TermIds with their value's cached.
         * Also fix the other tests in this class.  See TestSUM.
         */
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

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

        final IConstraint totalPriceConstraint = new SPARQLConstraint<XSDBooleanIV>(
                new CompareBOp(
                        totalPrice,
                        new Constant<XSDIntIV<BigdataLiteral>>(new XSDIntIV(10)),
                        CompareOp.GT));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
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
                                new IValueExpression[] { org }),
                        new NV(GroupByOp.Annotations.HAVING,//
                                new IConstraint[] { totalPriceConstraint })//
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

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
         * </pre>
         */
        
        // Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price21 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(21)));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org1, _price21 } )
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
        assertEquals(1, stats.unitsOut.get());
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
     * HAVING (SUM(?lprice) > 5)
     * </pre>
     * 
     * The solutions input to the GROUP_BY are:
     * 
     * <pre>
     * ?org  ?auth  ?book  ?lprice
     * org1  auth1  book1  9
     * org1  auth1  book3  5
     * org1  auth2  book3  blue
     * org2  auth3  book4  7
     * </pre>
     * 
     * The aggregated solutions groups are:
     * 
     * <pre>
     * ?org  ?totalPrice
     * org2  7
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * TODO Version without a HAVING clause (so it does not encounter the error
     * and thereby fail the group)
     */
    public void test_simple_groupBy_with_aggregate_and_having_with_errors() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");
        final IConstant<String> auth1 = new Constant<String>("auth1");
        final IConstant<String> auth2 = new Constant<String>("auth2");
        final IConstant<String> auth3 = new Constant<String>("auth3");
        final IConstant<String> book1 = new Constant<String>("book1");
        final IConstant<String> book2 = new Constant<String>("book2");
        final IConstant<String> book3 = new Constant<String>("book3");
        final IConstant<String> book4 = new Constant<String>("book4");
        final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(getName());
        final TermId tid1 = new TermId<BigdataValue>(VTE.LITERAL, 1);
        tid1.setValue(f.createLiteral("blue"));
        final IConstant<IV> blue = new Constant<IV>(tid1);
        final IConstant<XSDIntIV<BigdataLiteral>> price5 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(5));
        final IConstant<XSDIntIV<BigdataLiteral>> price7 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(7));
        final IConstant<XSDIntIV<BigdataLiteral>> price9 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(9));

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

        final IConstraint totalPriceConstraint = new SPARQLConstraint<XSDBooleanIV>(
                new CompareBOp(
                        totalPrice,
                        new Constant<XSDIntIV<BigdataLiteral>>(new XSDIntIV(5)),
                        CompareOp.GT));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
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
                                new IValueExpression[] { org }),
                        new NV(GroupByOp.Annotations.HAVING,//
                                new IConstraint[] { totalPriceConstraint })//
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

        /**
         * The test data:
         * 
         * <pre>
         * ?org  ?auth  ?book  ?lprice
         * org1  auth1  book1  9
         * org1  auth1  book3  5
         * org1  auth2  book3  blue
         * org2  auth3  book4  7
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, blue } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /**
         * The expected solutions:
         * 
         * <pre>
         * ?org  ?totalPrice
         * org2  7
         * </pre>
         */
        
        // Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price7 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(7)));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org2, _price7 } )
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
        assertEquals(1, stats.unitsOut.get());
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
     * org1  auth2  book3  blue
     * org2  auth3  book4  7
     * </pre>
     * 
     * The aggregated solutions groups are:
     * 
     * <pre>
     * ?org  ?totalPrice
     * org1  N/A           // Note: No value bound for ?totalPrice.
     * org2  7
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_groupBy_with_aggregate_no_having_with_errors() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

        final IConstant<String> org1 = new Constant<String>("org1");
        final IConstant<String> org2 = new Constant<String>("org2");
        final IConstant<String> auth1 = new Constant<String>("auth1");
        final IConstant<String> auth2 = new Constant<String>("auth2");
        final IConstant<String> auth3 = new Constant<String>("auth3");
        final IConstant<String> book1 = new Constant<String>("book1");
        final IConstant<String> book2 = new Constant<String>("book2");
        final IConstant<String> book3 = new Constant<String>("book3");
        final IConstant<String> book4 = new Constant<String>("book4");
        final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(getName());
        final TermId tid1 = new TermId<BigdataValue>(VTE.LITERAL, 1);
        tid1.setValue(f.createLiteral("blue"));
        final IConstant<IV> blue = new Constant<IV>(tid1);
        final IConstant<XSDIntIV<BigdataLiteral>> price5 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(5));
        final IConstant<XSDIntIV<BigdataLiteral>> price7 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(7));
        final IConstant<XSDIntIV<BigdataLiteral>> price9 = new Constant<XSDIntIV<BigdataLiteral>>(
                new XSDIntIV<BigdataLiteral>(9));

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

        final IVariableFactory variableFactory = new MockVariableFactory();
        
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
                                new IValueExpression[] { org }),
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

        /**
         * The test data:
         * 
         * <pre>
         * ?org  ?auth  ?book  ?lprice
         * org1  auth1  book1  9
         * org1  auth1  book3  5
         * org1  auth2  book3  blue
         * org2  auth3  book4  7
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, blue } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /**
         * The expected solutions:
         * 
         * <pre>
         * ?org  ?totalPrice
         * org1  N/A
         * org2  7
         * </pre>
         */
        
        // Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price7 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(7)));
        final IBindingSet expected[] = new IBindingSet[]
        {
                new ArrayBindingSet ( new IVariable<?> [] { org,            },  new IConstant [] { org1,         } ),
                new ArrayBindingSet ( new IVariable<?> [] { org, totalPrice },  new IConstant [] { org2, _price7 } )
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
     * SELECT (SUM(?lprice) AS ?totalPrice)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
     * HAVING (SUM(?lprice) > 10)
     * </pre>
     * 
     * The inputs are:
     * 
     * <pre>
     * ?org  ?auth  ?book  ?lprice
     * org1  auth1  book1  9
     * org1  auth1  book3  5
     * org1  auth2  book3  7
     * org2  auth3  book4  7
     * </pre>
     * 
     * The solutions are:
     * 
     * <pre>
     * ?totalPrice
     * 28
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_without_groupBy_with_aggregate_and_having() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

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

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

        final IConstraint totalPriceConstraint = new SPARQLConstraint<XSDBooleanIV>(
                new CompareBOp(
                        totalPrice,
                        new Constant<XSDIntIV<BigdataLiteral>>(new XSDIntIV(10)),
                        CompareOp.GT));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { totalPriceExpr }), //
                        new NV(GroupByOp.Annotations.HAVING,//
                                new IConstraint[] { totalPriceConstraint })//
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

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
         * ?totalPrice
         * 28
         * </pre>
         */
        
        // Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price28 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(28)));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { totalPrice },  new IConstant [] { _price28 } )
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
        assertEquals(1, stats.unitsOut.get());
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
     * SELECT (SUM(?lprice) AS ?totalPrice)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
     * HAVING (SUM(?lprice) > 50)
     * </pre>
     * 
     * The inputs are:
     * 
     * <pre>
     * ?org  ?auth  ?book  ?lprice
     * org1  auth1  book1  9
     * org1  auth1  book3  5
     * org1  auth2  book3  7
     * org2  auth3  book4  7
     * </pre>
     * 
     * There are NO solutions.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_without_groupBy_with_aggregate_and_having_no_solutions() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

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

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));

        final IConstraint totalPriceConstraint = new SPARQLConstraint<XSDBooleanIV>(
                new CompareBOp(
                        totalPrice,
                        new Constant<XSDIntIV<BigdataLiteral>>(new XSDIntIV(50)),
                        CompareOp.GT));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { totalPriceExpr }), //
                        new NV(GroupByOp.Annotations.HAVING,//
                                new IConstraint[] { totalPriceConstraint })//
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

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
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /**
         * THERE ARE NO SOLUTIONS.
         */
        
        final IBindingSet expected[] = new IBindingSet[] {};

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
        assertEquals(0, stats.unitsOut.get());
        assertEquals(0, stats.chunksOut.get());

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
     * The aggregated solution is:
     * 
     * <pre>
     * ?totalPrice
     * 28
     * </pre>
     * 
     * Note that you can not SELECT "?org" as there is no group by clause in
     * this example and "?org" is a variable at the detail rather than aggregate
     * level. (You could select SAMPLE(?org) or GROUP_CONCAT(?org).)
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_aggregate_no_groups() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

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

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(false/* distinct */, (IValueExpression<IV>) lprice));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { totalPriceExpr }) //
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

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
         * ?totalPrice
         * 28
         * </pre>
         */
        
        // Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price28 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(28)));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { totalPrice },  new IConstant [] { _price28 } )
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
        assertEquals(1, stats.unitsOut.get());
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
     * SELECT ?org, (SUM(DISTINCT ?lprice) AS ?totalPrice)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
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
     * The aggregated solution is:
     * 
     * <pre>
     * ?totalPrice
     * 21
     * </pre>
     * 
     * Note that you can not SELECT "?org" as there is no group by clause in
     * this example and "?org" is a variable at the detail rather than aggregate
     * level. (You could select SAMPLE(?org) or GROUP_CONCAT(?org).)
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_distinct_aggregate_no_groups() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> totalPrice = Var.var("totalPrice");

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

        final int groupById = 1;
        
        final IValueExpression<IV> totalPriceExpr = new Bind(totalPrice,
                new SUM(true/* distinct */, (IValueExpression<IV>) lprice));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { totalPriceExpr }) //
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

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
         * ?totalPrice
         * 21
         * </pre>
         */
        
        // Note: The aggregates will have gone through type promotion.
        final IConstant<XSDIntegerIV<BigdataLiteral>> _price21 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(21)));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { totalPrice },  new IConstant [] { _price21 } )
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
        assertEquals(1, stats.unitsOut.get());
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
     * SELECT ?org, (COUNT(*) AS ?totalCount)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
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
     * The aggregated solution is:
     * 
     * <pre>
     * ?totalCount
     * 4
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_count_star_no_groups() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> star = Var.var("*");
        final IVariable<IV> totalCount = Var.var("totalCount");

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

        final int groupById = 1;
        
        final IValueExpression<IV> totalCountExpr = new Bind(totalCount,
                new COUNT(false/* distinct */, star));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { totalCountExpr }) //
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

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
         * ?totalCount
         * 4
         * </pre>
         */
        
        final IConstant<XSDLongIV<BigdataLiteral>> _totalCount = new Constant<XSDLongIV<BigdataLiteral>>(
                new XSDLongIV<BigdataLiteral>(4));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { totalCount },  new IConstant [] { _totalCount } )
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
        assertEquals(1, stats.unitsOut.get());
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
     * SELECT ?org, (COUNT(DISTINCT *) AS ?totalCount)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
     * </pre>
     * 
     * The solutions input to the GROUP_BY are:
     * 
     * <pre>
     * ?org  ?auth  ?book  ?lprice
     * org1  auth1  book1  9
     * org1  auth1  book3  5
     * org1  auth2  book3  7
     * org1  auth2  book3  7 // duplicate record!
     * org2  auth3  book4  7
     * </pre>
     * 
     * The aggregated solution is:
     * 
     * <pre>
     * ?totalCount
     * 4
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_simple_count_distinct_star_no_groups() {

        final IVariable<IV> org = Var.var("org");
        final IVariable<IV> auth = Var.var("auth");
        final IVariable<IV> book = Var.var("book");
        final IVariable<IV> lprice = Var.var("lprice");
        final IVariable<IV> star = Var.var("*");
        final IVariable<IV> totalCount = Var.var("totalCount");

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

        final int groupById = 1;
        
        final IValueExpression<IV> totalCountExpr = new Bind(totalCount,
                new COUNT(true/* distinct */, star));
        
        final IVariableFactory variableFactory = new MockVariableFactory();
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.SELECT, //
                                new IValueExpression[] { totalCountExpr }) //
                })) {
            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };

        /**
         * The test data:
         * 
         * <pre>
         * ?org  ?auth  ?book  ?lprice
         * org1  auth1  book1  9
         * org1  auth1  book3  5
         * org1  auth2  book3  7
         * org1  auth2  book3  7 // duplicate record!
         * org2  auth3  book4  7
         * </pre>
         */
        final IBindingSet data [] = new IBindingSet []
        {
            new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book1, price9 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth1, book2, price5 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org1, auth2, book3, price7 } )
          , new ListBindingSet ( new IVariable<?> [] { org, auth, book, lprice }, new IConstant [] { org2, auth3, book4, price7 } )
        };

        /**
         * The expected solutions:
         * 
         * <pre>
         * ?totalCount
         * 4
         * </pre>
         */
        
        final IConstant<XSDLongIV<BigdataLiteral>> _totalCount = new Constant<XSDLongIV<BigdataLiteral>>(
                new XSDLongIV<BigdataLiteral>(4));
        final IBindingSet expected[] = new IBindingSet[]
        {
              new ArrayBindingSet ( new IVariable<?> [] { totalCount },  new IConstant [] { _totalCount } )
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
        assertEquals(5, stats.unitsIn.get());
        assertEquals(1, stats.unitsOut.get());
        assertEquals(1, stats.chunksOut.get());

    }

}
