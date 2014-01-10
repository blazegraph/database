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
/*
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.rto;

import java.util.Properties;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;

/**
 * Data driven test suite for the Runtime Query Optimizer (RTO) using BSBM data
 * and queries based on BSBM.
 * <p>
 * Note: Q6 is no longer run in BSBM (the query was dropped).
 * <p>
 * Note: Q9 is a simple DESCRIBE (too simple for the RTO). Sample query is:
 * 
 * <pre>
 * PREFIX rev: <http://purl.org/stuff/rev#>
 * 
 * DESCRIBE ?x
 * WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review576> rev:reviewer ?x }
 * </pre>
 * 
 * Note: Q12 is a UNION (too simple for the RTO). Sample query is:
 * 
 * <pre>
 * PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
 * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
 * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * 
 * SELECT ?property ?hasValue ?isValueOf
 * WHERE {
 *   { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer134> ?property ?hasValue }
 *   UNION
 *   { ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer134> }
 * }
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6440 2012-08-14 17:57:33Z thompsonbry $
 * 
 *          TODO BSBM uses a lot of filters, subgroups, and sub-selects. As we
 *          build up coverage for those constructions in the RTO, it will handle
 *          more of the query. As a result, the observed join orders (and even
 *          the #of joins that are considered) are likely to change.
 * 
 *          TODO BSBM is parameterized. We can generate more queries against the
 *          pc100 data set easily enough. In priciple, those queries might
 *          exhibit different correlations. However, the pc100 data set may be
 *          too small for any interesting correlations. In fact, it may be too
 *          small since the vertex estimates and cutoff joins may be exact
 *          before the RTO is run running. If so, then we need to go back and
 *          use a larger data set. However, the specific parameterized queries
 *          will remain valid against larger data sets since BSBM only adds more
 *          data when generating a larger data set. Of course, the number of
 *          solutions for the queries may change.
 */
public class TestRTO_BSBM extends AbstractRTOTestCase {

//    private final static Logger log = Logger.getLogger(TestRTO_LUBM.class);
    
    /**
     * 
     */
    public TestRTO_BSBM() {
    }

    /**
     * @param name
     */
    public TestRTO_BSBM(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(BigdataSail.Options.TRIPLES_MODE, "true");

        properties.setProperty(BigdataSail.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        return properties;
        
    }

    /**
     * BSBM Q1 against pc100.
     */
    public void test_BSBM_Q1_pc100() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q1", // testURI,
                "rto/BSBM-Q1.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q1.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 2, 4, 1, 3, 5 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q2 against pc100.
     */
    public void test_BSBM_Q2_pc100() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q2", // testURI,
                "rto/BSBM-Q2.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q2.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 3, 4, 5, 1, 2, 6, 7, 8, 9, 10, 11, 12 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q3 against pc100.
     */
    public void test_BSBM_Q3_pc100() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q3", // testURI,
                "rto/BSBM-Q3.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q3.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 2, 5, 1, 3, 4 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q4 against pc100.
     * <p>
     * Note: This query has TWO join groups that are sufficiently complex to run
     * the RTO. However, only one of the join groups is marked for RTO
     * optimization in order to keep the test harness simple. The test harness
     * assumes that there is a single JOIN group that is optimized by the RTO
     * and then verifies the join ordering within that join group. The test
     * harness breaks if there is more than one join group optimized by the RTO.
     */
    public void test_BSBM_Q4_pc100() throws Exception {
       
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q4", // testURI,
                "rto/BSBM-Q4.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q4.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 9, 6, 7, 8, 10, 11 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q5 on the pc100 data set.
     */
    public void test_BSBM_Q5_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q5", // testURI,
                "rto/BSBM-Q5.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q5.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 1, 3, 2, 5, 4, 7, 6 };

        assertSameJoinOrder(expected, helper);
        
    }

    /**
     * BSBM Q7 on the pc100 data set.
     * 
     * FIXME This fails in the RTO:
     * 
     * <pre>
     * org.openrdf.query.QueryEvaluationException: java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator.hasNext(Bigdata2Sesame2BindingSetIterator.java:188)
     *     at org.openrdf.query.impl.TupleQueryResultImpl.hasNext(TupleQueryResultImpl.java:90)
     *     at info.aduna.iteration.Iterations.addAll(Iterations.java:71)
     *     at org.openrdf.query.impl.MutableTupleQueryResult.<init>(MutableTupleQueryResult.java:86)
     *     at org.openrdf.query.impl.MutableTupleQueryResult.<init>(MutableTupleQueryResult.java:92)
     *     at com.bigdata.bop.engine.AbstractQueryEngineTestCase.compareTupleQueryResults(AbstractQueryEngineTestCase.java:738)
     *     at com.bigdata.rdf.sparql.ast.eval.AbstractDataAndSPARQLTestCase$AbsHelper.compareTupleQueryResults(AbstractDataAndSPARQLTestCase.java:119)
     *     at com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase$TestHelper.compareTupleQueryResults(AbstractDataDrivenSPARQLTestCase.java:498)
     *     at com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase$TestHelper.runTest(AbstractDataDrivenSPARQLTestCase.java:320)
     *     at com.bigdata.rdf.sparql.ast.eval.rto.AbstractRTOTestCase.assertSameJoinOrder(AbstractRTOTestCase.java:181)
     *     at com.bigdata.rdf.sparql.ast.eval.rto.TestRTO_BSBM.test_BSBM_Q6_pc100(TestRTO_BSBM.java:198)
     *     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     *     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
     *     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     *     at java.lang.reflect.Method.invoke(Method.java:601)
     *     at junit.framework.TestCase.runTest(TestCase.java:154)
     *     at junit.framework.TestCase.runBare(TestCase.java:127)
     *     at junit.framework.TestResult$1.protect(TestResult.java:106)
     *     at junit.framework.TestResult.runProtected(TestResult.java:124)
     *     at junit.framework.TestResult.run(TestResult.java:109)
     *     at junit.framework.TestCase.run(TestCase.java:118)
     *     at org.eclipse.jdt.internal.junit.runner.junit3.JUnit3TestReference.run(JUnit3TestReference.java:130)
     *     at org.eclipse.jdt.internal.junit.runner.TestExecution.run(TestExecution.java:38)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.runTests(RemoteTestRunner.java:467)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.runTests(RemoteTestRunner.java:683)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.run(RemoteTestRunner.java:390)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.main(RemoteTestRunner.java:197)
     * Caused by: java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.relation.accesspath.BlockingBuffer$BlockingIterator.checkFuture(BlockingBuffer.java:1523)
     *     at com.bigdata.relation.accesspath.BlockingBuffer$BlockingIterator._hasNext(BlockingBuffer.java:1710)
     *     at com.bigdata.relation.accesspath.BlockingBuffer$BlockingIterator.hasNext(BlockingBuffer.java:1563)
     *     at com.bigdata.striterator.AbstractChunkedResolverator._hasNext(AbstractChunkedResolverator.java:357)
     *     at com.bigdata.striterator.AbstractChunkedResolverator.hasNext(AbstractChunkedResolverator.java:333)
     *     at com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator.hasNext(Bigdata2Sesame2BindingSetIterator.java:134)
     *     ... 26 more
     * Caused by: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:252)
     *     at java.util.concurrent.FutureTask.get(FutureTask.java:111)
     *     at com.bigdata.relation.accesspath.BlockingBuffer$BlockingIterator.checkFuture(BlockingBuffer.java:1454)
     *     ... 31 more
     * Caused by: java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.rdf.sail.RunningQueryCloseableIterator.checkFuture(RunningQueryCloseableIterator.java:59)
     *     at com.bigdata.rdf.sail.RunningQueryCloseableIterator.close(RunningQueryCloseableIterator.java:73)
     *     at com.bigdata.rdf.sail.RunningQueryCloseableIterator.hasNext(RunningQueryCloseableIterator.java:82)
     *     at com.bigdata.striterator.ChunkedWrappedIterator.hasNext(ChunkedWrappedIterator.java:197)
     *     at com.bigdata.striterator.AbstractChunkedResolverator$ChunkConsumerTask.call(AbstractChunkedResolverator.java:222)
     *     at com.bigdata.striterator.AbstractChunkedResolverator$ChunkConsumerTask.call(AbstractChunkedResolverator.java:1)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:334)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:166)
     *     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1110)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:603)
     *     at java.lang.Thread.run(Thread.java:722)
     * Caused by: java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.util.concurrent.Haltable.get(Haltable.java:273)
     *     at com.bigdata.bop.engine.AbstractRunningQuery.get(AbstractRunningQuery.java:1474)
     *     at com.bigdata.bop.engine.AbstractRunningQuery.get(AbstractRunningQuery.java:1)
     *     at com.bigdata.rdf.sail.RunningQueryCloseableIterator.checkFuture(RunningQueryCloseableIterator.java:46)
     *     ... 10 more
     * Caused by: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.bop.engine.ChunkedRunningQuery.scheduleNext(ChunkedRunningQuery.java:678)
     *     at com.bigdata.bop.engine.ChunkedRunningQuery.acceptChunk(ChunkedRunningQuery.java:290)
     *     at com.bigdata.bop.engine.QueryEngine.acceptChunk(QueryEngine.java:1031)
     *     at com.bigdata.bop.engine.QueryEngine.startEval(QueryEngine.java:1697)
     *     at com.bigdata.bop.engine.QueryEngine.eval(QueryEngine.java:1564)
     *     at com.bigdata.bop.engine.QueryEngine.eval(QueryEngine.java:1470)
     *     at com.bigdata.bop.engine.QueryEngine.eval(QueryEngine.java:1447)
     *     at com.bigdata.bop.controller.JVMNamedSubqueryOp$ControllerTask$SubqueryTask.call(JVMNamedSubqueryOp.java:361)
     *     at com.bigdata.bop.controller.JVMNamedSubqueryOp$ControllerTask.call(JVMNamedSubqueryOp.java:278)
     *     at com.bigdata.bop.controller.JVMNamedSubqueryOp$ControllerTask.call(JVMNamedSubqueryOp.java:1)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:334)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:166)
     *     at com.bigdata.bop.engine.ChunkedRunningQuery$ChunkTask.call(ChunkedRunningQuery.java:1301)
     *     at com.bigdata.bop.engine.ChunkedRunningQuery$ChunkTaskWrapper.run(ChunkedRunningQuery.java:856)
     *     at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:334)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:166)
     *     at com.bigdata.concurrent.FutureTaskMon.run(FutureTaskMon.java:63)
     *     at com.bigdata.bop.engine.ChunkedRunningQuery$ChunkFutureTask.run(ChunkedRunningQuery.java:751)
     *     ... 3 more
     * Caused by: java.lang.RuntimeException: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.bop.engine.ChunkedRunningQuery.scheduleNext(ChunkedRunningQuery.java:648)
     *     ... 21 more
     * Caused by: java.lang.AssertionError: No stats: op=com.bigdata.bop.join.JVMSolutionSetHashJoinOp[7]()[ com.bigdata.bop.BOp.bopId=7, com.bigdata.bop.BOp.evaluationContext=CONTROLLER, com.bigdata.bop.PipelineOp.sharedState=true, namedSetRef=NamedSolutionSetRef{localName=--nsr-1,queryId=6690c373-8ff2-44b7-826c-f80d8e24eec2,joinVars=[]}, com.bigdata.bop.join.JoinAnnotations.constraints=null, class com.bigdata.bop.join.SolutionSetHashJoinOp.release=false]
     *     at com.bigdata.bop.engine.ChunkedRunningQuery$ChunkTask.<init>(ChunkedRunningQuery.java:1172)
     *     at com.bigdata.bop.engine.ChunkedRunningQuery.scheduleNext(ChunkedRunningQuery.java:640)
     *     ... 21 more
     * </pre>
     */
    public void test_BSBM_Q7_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q7", // testURI,
                "rto/BSBM-Q7.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q7.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        // FIXME The join order is unknown. This query does not run through the RTO yet.
        final int[] expected = new int[] { 1, 3, 2, 5, 4, 7, 6 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q8 on the pc100 data set.
     */
    public void test_BSBM_Q8_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q8", // testURI,
                "rto/BSBM-Q8.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q8.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 1, 3, 2, 4, 5, 6 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q10 on pc100.
     */
    public void test_BSBM_Q10_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q10", // testURI,
                "rto/BSBM-Q10.rq",// queryFileURL
                "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q10.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 1, 7, 5, 2, 3, 4, 6 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /*
     * larger runs -- OOM on laptop when running under Eclipse.
     */
    
//    /**
//     * BSBM Q1 against pc1000 (OOM on laptop).
//     */
//    public void _test_BSBM_Q1_pc1000() throws Exception {
//
//        final TestHelper helper = new TestHelper(//
//                "rto/BSBM-Q1", // testURI,
//                "rto/BSBM-Q1.rq",// queryFileURL
//                "bigdata-rdf/src/resources/data/bsbm/dataset_pc1000.nt.gz",// dataFileURL
//                "rto/BSBM-Q1.srx"// resultFileURL
//        );
//        
//        /*
//         * Verify that the runtime optimizer produced the expected join path.
//         */
//
//        final int[] expected = new int[] { 3, 2, 4, 1, 5 };
//
//        assertSameJoinOrder(expected, helper);
//        
//    }    
    
}
