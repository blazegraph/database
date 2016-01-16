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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.AndBOp;
import com.bigdata.rdf.internal.constraints.FalseBOp;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.constraints.OrBOp;
import com.bigdata.rdf.internal.constraints.SparqlTypeErrorBOp;
import com.bigdata.rdf.internal.constraints.TrueBOp;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.IRule;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id: TestOptionals.java 3149 2010-07-07 01:19:11Z mrpersonick $
 */
public class TestInlineConstraints extends AbstractDataDrivenSPARQLTestCase {

//	private static final Logger log = Logger.getLogger(TestInlineConstraints.class);
	
    /**
     * 
     */
    public TestInlineConstraints() {
        super();
    }

    /**
     * @param name
     */
    public TestInlineConstraints(String name) {
        super(name);
    }
    
    @Override
    public Properties getProperties() {
    	final Properties props = super.getProperties();
    	props.setProperty(BigdataSail.Options.INLINE_DATE_TIMES, "true");
    	return props;
    }
    
    public void testGT() throws Exception {
        
    	new TestHelper("inline-constraints-gt").runTest();
        
    }

    public void testGE() throws Exception {
        
    	new TestHelper("inline-constraints-ge").runTest();
        
    }

    public void testLT() throws Exception {
        
    	new TestHelper("inline-constraints-lt").runTest();
        
    }

    public void testLE() throws Exception {

    	new TestHelper("inline-constraints-le").runTest();
        
    }

    public void testMath() throws Exception {
        
    	new TestHelper("inline-constraints-math").runTest();
        
    }

    public void testCompareDates() throws Exception {
        
    	new TestHelper("inline-constraints-dt").runTest();
        
    }
    
    public void testAnd() {
    	
    	final IValueExpression T = TrueBOp.INSTANCE;
    	
    	final IVValueExpression F = FalseBOp.INSTANCE;
    	
    	final IVValueExpression E = SparqlTypeErrorBOp.INSTANCE;
		
		final IBindingSet bs = new ListBindingSet();
		
		{
			final AndBOp and = new AndBOp(T, T);
			final XSDBooleanIV iv = and.get(bs);
			assertTrue(iv.booleanValue());
		}
		{
			final AndBOp and = new AndBOp(T, F);
			final XSDBooleanIV iv = and.get(bs);
			assertFalse(iv.booleanValue());
		}
		{
			final AndBOp and = new AndBOp(F, T);
			final XSDBooleanIV iv = and.get(bs);
			assertFalse(iv.booleanValue());
		}
		{
			final AndBOp and = new AndBOp(F, F);
			final XSDBooleanIV iv = and.get(bs);
			assertFalse(iv.booleanValue());
		}
		{
			final AndBOp and = new AndBOp(T, E);
			try {
				and.get(bs);
				//should produce a type error
				assertTrue(false);
			} catch (SparqlTypeErrorException ex) { }
		}
		{
			final AndBOp and = new AndBOp(E, T);
			try {
				and.get(bs);
				//should produce a type error
				assertTrue(false);
			} catch (SparqlTypeErrorException ex) { }
		}
		{
			final AndBOp and = new AndBOp(E, F);
			final XSDBooleanIV iv = and.get(bs);
			assertFalse(iv.booleanValue());
		}
		{
			final AndBOp and = new AndBOp(F, E);
			final XSDBooleanIV iv = and.get(bs);
			assertFalse(iv.booleanValue());
		}
		{
			final AndBOp and = new AndBOp(E, E);
			try {
				and.get(bs);
				//should produce a type error
				assertTrue(false);
			} catch (SparqlTypeErrorException ex) { }
		}
    	
    }
    
    public void testOr() {
    	
    	final IValueExpression T = TrueBOp.INSTANCE;
    	
    	final IVValueExpression F = FalseBOp.INSTANCE;
    	
    	final IVValueExpression E = SparqlTypeErrorBOp.INSTANCE;
		
		final IBindingSet bs = new ListBindingSet();
		
		{
			final OrBOp or = new OrBOp(T, T);
			final XSDBooleanIV iv = or.get(bs);
			assertTrue(iv.booleanValue());
		}
		{
			final OrBOp or = new OrBOp(T, F);
			final XSDBooleanIV iv = or.get(bs);
			assertTrue(iv.booleanValue());
		}
		{
			final OrBOp or = new OrBOp(F, T);
			final XSDBooleanIV iv = or.get(bs);
			assertTrue(iv.booleanValue());
		}
		{
			final OrBOp or = new OrBOp(F, F);
			final XSDBooleanIV iv = or.get(bs);
			assertFalse(iv.booleanValue());
		}
		{
			final OrBOp or = new OrBOp(E, T);
			final XSDBooleanIV iv = or.get(bs);
			assertTrue(iv.booleanValue());
		}
		{
			final OrBOp or = new OrBOp(T, E);
			final XSDBooleanIV iv = or.get(bs);
			assertTrue(iv.booleanValue());
		}
		{
			final OrBOp or = new OrBOp(F, E);
			try {
				or.get(bs);
				//should produce a type error
				assertTrue(false);
			} catch (SparqlTypeErrorException ex) { }
		}
		{
			final OrBOp or = new OrBOp(E, F);
			try {
				or.get(bs);
				//should produce a type error
				assertTrue(false);
			} catch (SparqlTypeErrorException ex) { }
		}
		{
			final OrBOp or = new OrBOp(E, E);
			try {
				or.get(bs);
				//should produce a type error
				assertTrue(false);
			} catch (SparqlTypeErrorException ex) { }
		}
		
    }

//    private IChunkedOrderedIterator<ISolution> runQuery(AbstractTripleStore db, IRule rule)
//        throws Exception {
//        // run the query as a native rule.
//        final IEvaluationPlanFactory planFactory =
//                DefaultEvaluationPlanFactory2.INSTANCE;
//        final IJoinNexusFactory joinNexusFactory =
//                db.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
//                        ActionEnum.Query, IJoinNexus.BINDINGS, null, // filter
//                        false, // justify 
//                        false, // backchain
//                        planFactory);
//        final IJoinNexus joinNexus =
//                joinNexusFactory.newInstance(db.getIndexManager());
//        final IEvaluationPlan plan = planFactory.newPlan(joinNexus, rule);
//        StringBuilder sb = new StringBuilder();
//        int order[] = plan.getOrder();
//        for (int i = 0; i < order.length; i++) {
//            sb.append(order[i]);
//            if (i < order.length-1) {
//                sb.append(",");
//            }
//        }
//        if(log.isInfoEnabled())log.info("order: [" + sb.toString() + "]");
//        IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(rule);
//        return solutions;
//    }
    
    private IPredicate toPredicate(final AbstractTripleStore database,
    		final AtomicInteger idFactory,
    		final IVariableOrConstant<IV> s,
    		final IVariableOrConstant<IV> p,
    		final IVariableOrConstant<IV> o) {
    	
        // The annotations for the predicate.
        final List<NV> anns = new LinkedList<NV>();

        // Decide on the correct arity for the predicate.
        final BOp[] vars = new BOp[] { s, p, o };

        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
                new String[] { database.getSPORelation().getNamespace() }));//
        

        // timestamp
        anns.add(new NV(Annotations.TIMESTAMP, database
                .getSPORelation().getTimestamp()));

        /*
         * Explicitly set the access path / iterator flags.
         * 
         * Note: High level query generally permits iterator level parallelism.
         * We set the PARALLEL flag here so it can be used if a global index
         * view is chosen for the access path.
         * 
         * Note: High level query for SPARQL always uses read-only access paths.
         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
         * semantics then you will need to remote the READONLY flag for the
         * mutable access paths.
         */
        anns.add(new NV(IPredicate.Annotations.FLAGS, IRangeQuery.DEFAULT
                | IRangeQuery.PARALLEL | IRangeQuery.READONLY));
        
        anns.add(new NV(BOp.Annotations.BOP_ID, idFactory.incrementAndGet()));
        
        return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));
    	
    }
    		
    		
    
    private IChunkedOrderedIterator<IBindingSet> runQuery(
    		final AbstractTripleStore db, final IRule rule)
    		throws Exception {

        fail("refactor test suite");

        return null;
//        final QueryEngine queryEngine = 
//	    	QueryEngineFactory.getQueryController(db.getIndexManager());
//	
//		final PipelineOp query;
//		{
//			/*
//			 * Note: The ids are assigned using incrementAndGet() so ONE (1) is
//			 * the first id that will be assigned when we pass in ZERO (0) as
//			 * the initial state of the AtomicInteger.
//			 */
//			final AtomicInteger idFactory = new AtomicInteger(0);
//	
//			// Convert the step to a bigdata operator tree.
//			query = Rule2BOpUtility.convert(rule, idFactory, db,
//					queryEngine, new Properties());
//	
//			if (log.isInfoEnabled())
//				log.info("\n"+BOpUtility.toString2(query));
//	
//		}
//		
//	    IRunningQuery runningQuery = null;
//    	try {
//    		
//    		// Submit query for evaluation.
//    		runningQuery = queryEngine.eval(query);
//
//    		// The iterator draining the query solutions.
//    		final IAsynchronousIterator<IBindingSet[]> it1 = runningQuery
//    				.iterator();
//
//    	    // De-chunk the IBindingSet[] visited by that iterator.
//    	    final IChunkedOrderedIterator<IBindingSet> it2 = 
//    	    	new ChunkedWrappedIterator<IBindingSet>(
//    	            new Dechunkerator<IBindingSet>(it1));
//    	    
//    	    return it2;
//
//		} catch (Throwable t) {
//			if (runningQuery != null) {
//				// ensure query is halted.
//				runningQuery.cancel(true/* mayInterruptIfRunning */);
//			}
////			log.error("Remove log stmt"+t,t);// FIXME remove this - I am just looking for the root cause of something in the SAIL.
//			throw new QueryEvaluationException(t);
//		}
		
    }

}
