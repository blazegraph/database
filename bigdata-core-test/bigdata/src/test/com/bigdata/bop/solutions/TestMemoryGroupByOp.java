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
package com.bigdata.bop.solutions;

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
import com.bigdata.bop.TestMockUtility;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.rdf.aggregate.COUNT;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import java.math.BigInteger;
import java.util.concurrent.FutureTask;
import static junit.framework.TestCase.assertEquals;

/**
 * Unit tests for {@link MemoryGroupByOp}.
 * 
 * @author thompsonbry
 */
public class TestMemoryGroupByOp extends AbstractAggregationTestCase {
    
	public TestMemoryGroupByOp() {
	}

	public TestMemoryGroupByOp(String name) {
		super(name);
	}
        
    @Override
    protected GroupByOp newFixture(IValueExpression<?>[] select,
            IValueExpression<?>[] groupBy, IConstraint[] having) {

        final int groupById = 1;

        final IVariableFactory variableFactory = new MockVariableFactory();

        final IGroupByState groupByState = new GroupByState(//
                select, groupBy, having);

        final IGroupByRewriteState groupByRewrite = new GroupByRewriter(
                groupByState) {

            private static final long serialVersionUID = 1L;

            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.GROUP_BY_STATE, groupByState), //
                        new NV(GroupByOp.Annotations.GROUP_BY_REWRITE, groupByRewrite), //
                }));

        return query;
    }

    @Override
    protected boolean isPipelinedAggregationOp() {
        return false;
    }
    
    
    /**
     * A variant of
     * 
     * https://www.w3.org/2009/sparql/docs/tests/data-sparql11/grouping/group03.rq
     * 
     * with DISTINCT in an aggregate. The test is not intended for 
     * PipelinedAggregationOp because PipelinedAggregationOp does not support
     * DISTINCT, so it is here rather than in AbstractAggregationTestCase.
     * 
     * <pre>
     * @prefix : <http://example/> .
     *
     * :s1 :p 1 .
     * :s1 :q 9 .
     * :s2 :p 2 . 
     * </pre>
     * 
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT ?w (COUNT(DISTINCT ?v) AS ?S)
     * {
     *   ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     * </pre>
     * 
     * The solutions input to the GROUP_BY are:
     * 
     * <pre>
     * ?w  ?s  ?v
     *  9  s1   1
     *     s2   2
     * </pre>
     * 
     * The aggregated solutions groups are:
     * 
     * <pre>
     * ?w  ?S  
     *  9   1  
     *      1  
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_aggregation_groupBy_by_error_values3() {
                 
       AbstractTripleStore kb = TestMockUtility.mockTripleStore(getName());
       try {
           final String lexiconNamespace = kb.getLexiconRelation().getNamespace();
           final GlobalAnnotations globals = new GlobalAnnotations(lexiconNamespace, ITx.READ_COMMITTED);

           final IVariable<IV> w = Var.var("w");
           final IVariable<IV> v = Var.var("v");
           final IVariable<IV> S = Var.var("S");
           final IVariable<IV> s = Var.var("s");
   
           final IConstant<String> s1 = new Constant<String>("s1");
           final IConstant<String> s2 = new Constant<String>("s2");
           final IConstant<XSDNumericIV<BigdataLiteral>> num1 = new Constant<XSDNumericIV<BigdataLiteral>>(
                   new XSDNumericIV<BigdataLiteral>(1));
           final IConstant<XSDNumericIV<BigdataLiteral>> num2 = new Constant<XSDNumericIV<BigdataLiteral>>(
                   new XSDNumericIV<BigdataLiteral>(2));
           final IConstant<XSDNumericIV<BigdataLiteral>> num9 = new Constant<XSDNumericIV<BigdataLiteral>>(
                   new XSDNumericIV<BigdataLiteral>(9));
           
           // COUNT(DISTINCT ?v) AS ?S
           final IValueExpression<IV> countDistinctVAsS = new Bind(S,
                   new COUNT(true/* distinct */, (IValueExpression<IV>) v));
   
   
           
           final GroupByOp query = newFixture(//
                       new IValueExpression[] { w, countDistinctVAsS }, // select
                       new IValueExpression[] { w }, // groupBy
                       null // having
               );
           
        
           /**
            * The test data:
            *
            * <pre>
            * ?w  ?s  ?v
            *  9  s1   1
            *     s2   2
            * </pre>
            */
           final IBindingSet data [] = new IBindingSet []
           {
               new ListBindingSet ( new IVariable<?> [] { w, s, v }, new IConstant [] { num9, s1, num1 } )
             , new ListBindingSet ( new IVariable<?> [] { s, v }, new IConstant [] { s2, num2 } )
           };
   
     
           /**
            * The expected solutions:
            *
            * <pre>
            * ?w  ?S
            *  9   1
            *      1
            * </pre>
            *
            * </pre>
            */
                      // Note: The aggregates will have gone through type promotion.
           final IConstant<XSDIntegerIV<BigdataLiteral>> _num1 = new Constant<XSDIntegerIV<BigdataLiteral>>(
                   new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(1)));
        
           final IBindingSet expected[] = new IBindingSet[]
           {
                   new ListBindingSet ( new IVariable<?> [] { w, S },  new IConstant [] { num9, _num1 } ),
                   new ListBindingSet ( new IVariable<?> [] { S },  new IConstant [] { _num1 } )
           };
   
           final BOpStats stats = query.newStats();
   
           final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                   new IBindingSet[][] { data });
   
           final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                   query, stats);
   
           final IRunningQuery runningQuery = new MockRunningQuery(null/* fed */
           , kb.getIndexManager()/* indexManager */
           , queryContext
           );
   
           // Note: [lastInvocation:=true] forces the solutions to be emitted.
           final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                   runningQuery, -1/* partitionId */
                   , stats, query/* op */, true/* lastInvocation */, source, sink,
                   null/* sink2 */
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
   
           // Check the solutions.
           AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected, sink.iterator(),
                   ft);           
           
           assertEquals(1, stats.chunksIn.get());
           assertEquals(2, stats.unitsIn.get());
           assertEquals(2, stats.unitsOut.get());
           assertEquals(1, stats.chunksOut.get());
       } finally {
          kb.getIndexManager().destroy();
       }
        
        
    } // test_aggregation_groupBy_by_error_values3()

}
