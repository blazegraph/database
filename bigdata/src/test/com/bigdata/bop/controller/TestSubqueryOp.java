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
 * Created on Aug 23, 2010
 */

package com.bigdata.bop.controller;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Dechunkerator;

/**
 * Test suite for handling of optional join groups during query evaluation
 * against a local database instance.
 * 
 * <pre>
 * -Dlog4j.configuration=bigdata/src/resources/logging/log4j.properties
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestSubqueryOp extends AbstractSubqueryTestCase {

    /**
     * 
     */
    public TestSubqueryOp() {
    }

    /**
     * @param name
     */
    public TestSubqueryOp(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;
        
    }

    static private final String namespace = "ns";
    private Journal jnl;
    private QueryEngine queryEngine;
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

        loadData(jnl);
        
        queryEngine = new QueryEngine(jnl);
        
        queryEngine.init();

    }
    
    /**
     * Create and populate relation in the {@link #namespace}.
     */
    private void loadData(final Journal store) {

        // create the relation.
        final R rel = new R(store, namespace, ITx.UNISOLATED, new Properties());
        rel.create();

        // data to insert (in key order for convenience).
        final E[] a = {//
                new E("Paul", "Mary"),// [0]
                new E("Paul", "Brad"),// [1]
                
                new E("John", "Mary"),// [2]
                new E("John", "Brad"),// [3]
                
                new E("Mary", "Brad"),// [4]
                
                new E("Brad", "Fred"),// [5]
                new E("Brad", "Leon"),// [6]
        };

        // insert data (the records are not pre-sorted).
        rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        // Do commit since not scale-out.
        store.commit();

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
     * Unit test for optional join group. Three joins are used and target a
     * {@link SliceOp}. The 2nd and 3rd joins are embedded in an
     * {@link SubqueryOp}.
     * <P>
     * The optional join group takes the form:
     * 
     * <pre>
     * (a b)
     * optional { 
     *   (b c) 
     *   (c d) 
     * }
     * </pre>
     * 
     * The (a b) tail will match everything in the knowledge base. The join
     * group takes us two hops out from ?b. There should be four solutions that
     * succeed the optional join group:
     * 
     * <pre>
     * (paul mary brad fred)
     * (paul mary brad leon)
     * (john mary brad fred)
     * (john mary brad leon)
     * </pre>
     * 
     * and five more that don't succeed the optional join group:
     * 
     * <pre>
     * (paul brad) *
     * (john brad) *
     * (mary brad) *
     * (brad fred)
     * (brad leon)
     * </pre>
     * 
     * In this cases marked with a <code>*</code>, ?c will become temporarily
     * bound to fred and leon (since brad knows fred and leon), but the (c d)
     * tail will fail since fred and leon don't know anyone else. At this point,
     * the ?c binding must be removed from the solution.
     */
    public void test_query_join2_optionals() throws Exception {

        // main query
        final int startId = 1; // 
        final int joinId1 = 2; //         : base join group.
        final int predId1 = 3; // (a b)
        final int joinGroup1 = 9;
        final int sliceId = 8; // 

        // subquery
        final int joinId2 = 4; //         : joinGroup1
        final int predId2 = 5; // (b c)   
        final int joinId3 = 6; //         : joinGroup1
        final int predId3 = 7; // (c d)

        final IVariable<?> a = Var.var("a");
        final IVariable<?> b = Var.var("b");
        final IVariable<?> c = Var.var("c");
        final IVariable<?> d = Var.var("d");

        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(
                new IVariableOrConstant[] { a, b }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(
                new IVariableOrConstant[] { b, c }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred3Op = new Predicate<E>(
                new IVariableOrConstant[] { c, d }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId3),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final PipelineOp join1Op = new PipelineJoin<E>(//
                new BOp[]{startOp},// 
                        new NV(Predicate.Annotations.BOP_ID, joinId1),//
                        new NV(PipelineJoin.Annotations.PREDICATE,pred1Op));

        final PipelineOp subQuery;
        {
        final PipelineOp join2Op = new PipelineJoin<E>(//
                new BOp[] { /*join1Op*/ },//
                new NV(Predicate.Annotations.BOP_ID, joinId2),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
                new NV(PipelineJoin.Annotations.PREDICATE, pred2Op)//
//                // join is optional.
//                new NV(PipelineJoin.Annotations.OPTIONAL, true),//
//                // optional target is the same as the default target.
//                new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
                );

        final PipelineOp join3Op = new PipelineJoin<E>(//
                new BOp[] { join2Op },//
                new NV(Predicate.Annotations.BOP_ID, joinId3),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
                new NV(PipelineJoin.Annotations.PREDICATE, pred3Op)//
//                // join is optional.
//                new NV(PipelineJoin.Annotations.OPTIONAL, true),//
//                // optional target is the same as the default target.
//                new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
                );
        subQuery = join3Op;
        }

        final PipelineOp joinGroup1Op = new SubqueryOp(new BOp[]{join1Op}, 
                new NV(Predicate.Annotations.BOP_ID, joinGroup1),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
                new NV(SubqueryOp.Annotations.SUBQUERY, subQuery),//
//                , new NV(BOp.Annotations.CONTROLLER,true)//
//                new NV(BOp.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.CONTROLLER)//
                // join is optional.
                new NV(SubqueryOp.Annotations.OPTIONAL, true)//
//                // optional target is the same as the default target.
//                new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
        );
        
        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{joinGroup1Op},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        }));

        final PipelineOp query = sliceOp;

        // start the query.
        final UUID queryId = UUID.randomUUID();
        final IChunkMessage<IBindingSet> initialChunkMessage;
        {

            final IBindingSet initialBindings = new HashBindingSet();

//            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
                    queryId, startId,//
                    -1, // partitionId
                    newBindingSetIterator(initialBindings));
        }
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialChunkMessage);

        // verify solutions.
        {

            // the expected solutions.
            final IBindingSet[] expected = new IBindingSet[] {//
            // four solutions where the optional join succeeds.
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Fred") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Leon") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Fred") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Leon") }//
            ),
            // plus anything we read from the first access path which did not 
            // pass the optional join
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Brad"),
                            new Constant<String>("Fred") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Brad"),
                            new Constant<String>("Leon") }//
            )
            };

            /*
             * junit.framework.AssertionFailedError: Iterator will deliver too
             * many objects: reminder(3)=[{ a=John, b=Brad }, { a=Mary, b=Brad
             * }, { a=Paul, b=Brad }].
             */
            assertSameSolutionsAnyOrder(expected,
                    new Dechunkerator<IBindingSet>(runningQuery.iterator()));
        
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(4, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

    }

    /**
     * Unit test for optional join group with a filter. Three joins are used and
     * target a {@link SliceOp}. The 2nd and 3rd joins are embedded in an
     * optional join group. The optional join group contains a filter.
     * <p>
     * The optional join group takes the form:
     * 
     * <pre>
     * (a b)
     * optional { 
     *   (b c) 
     *   (c d) 
     *   filter(d != Leon) 
     * }
     * </pre>
     * 
     * The (a b) tail will match everything in the knowledge base. The join
     * group takes us two hops out from ?b. There should be two solutions that
     * succeed the optional join group:
     * 
     * <pre>
     * (paul mary brad fred)
     * (john mary brad fred)
     * </pre>
     * 
     * and five more that don't succeed the optional join group:
     * 
     * <pre>
     * (paul brad) *
     * (john brad) *
     * (mary brad) *
     * (brad fred)
     * (brad leon)
     * </pre>
     * 
     * In the cases marked with a <code>*</code>, ?c will become temporarily
     * bound to fred and leon (since brad knows fred and leon), but the (c d)
     * tail will fail since fred and leon don't know anyone else. At this point,
     * the ?c binding must be removed from the solution.
     * <p>
     * The filter (d != Leon) will prune the two solutions:
     * 
     * <pre>
     * (paul mary brad leon)
     * (john mary brad leon)
     * </pre>
     * 
     * since ?d is bound to Leon in those cases.
     */
    public void test_query_optionals_filter() throws Exception {

        // main query
        final int startId = 1;
        final int joinId1 = 2; // 
        final int predId1 = 3; // (a,b)
        final int joinGroup1 = 9;
        final int sliceId = 8;
        
        // subquery
        final int joinId2 = 4; //       : group1
        final int predId2 = 5; // (b,c)
        final int joinId3 = 6; //       : group1
        final int predId3 = 7; // (c,d) 

        
        final IVariable<?> a = Var.var("a");
        final IVariable<?> b = Var.var("b");
        final IVariable<?> c = Var.var("c");
        final IVariable<?> d = Var.var("d");

        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(
                new IVariableOrConstant[] { a, b }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(
                new IVariableOrConstant[] { b, c }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred3Op = new Predicate<E>(
                new IVariableOrConstant[] { c, d }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId3),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final PipelineOp join1Op = new PipelineJoin<E>(//
                new BOp[]{startOp},// 
                        new NV(Predicate.Annotations.BOP_ID, joinId1),//
                        new NV(PipelineJoin.Annotations.PREDICATE,pred1Op));

        final PipelineOp subQuery;
        {
		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { /*join1Op*/ },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op)//
//				// join is optional.
//				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
//				// optional target is the same as the default target.
//				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
				);

		final PipelineOp join3Op = new PipelineJoin<E>(//
				new BOp[] { join2Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId3),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred3Op),//
				// constraint d != Leon
				new NV(PipelineJoin.Annotations.CONSTRAINTS,
						new IConstraint[] { Constraint.wrap(new NEConstant(d, new Constant<String>("Leon"))) })
//				// join is optional.
//				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
//				// optional target is the same as the default target.
//				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
				);
		
		subQuery = join3Op;
        }
        
        final PipelineOp joinGroup1Op = new SubqueryOp(new BOp[]{join1Op}, 
                new NV(Predicate.Annotations.BOP_ID, joinGroup1),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
                new NV(SubqueryOp.Annotations.SUBQUERY, subQuery),//
//                new NV(BOp.Annotations.CONTROLLER,true)//
//                new NV(BOp.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.CONTROLLER)//
                // join is optional.
                new NV(SubqueryOp.Annotations.OPTIONAL, true)//
//                // optional target is the same as the default target.
//                new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
        );

        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{joinGroup1Op},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        }));

        final PipelineOp query = sliceOp;

        // start the query.
        final UUID queryId = UUID.randomUUID();
        final IChunkMessage<IBindingSet> initialChunkMessage;
        {

            final IBindingSet initialBindings = new HashBindingSet();

//            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
                    queryId, startId,//
                    -1, // partitionId
                    newBindingSetIterator(initialBindings));
        }
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialChunkMessage);

        // verify solutions.
        {

            // the expected solutions.
            final IBindingSet[] expected = new IBindingSet[] {//
            // two solutions where the optional join succeeds.
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Fred") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Fred") }//
            ),
            // plus anything we read from the first access path which did not 
            // pass the optional join
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Brad"),
                            new Constant<String>("Fred") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Brad"),
                            new Constant<String>("Leon") }//
            )
            };

            assertSameSolutionsAnyOrder(expected,
                    new Dechunkerator<IBindingSet>(runningQuery.iterator()));
        
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(4, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

    }

    /**
     * Unit test for optional join group with a filter on a variable outside the
     * optional join group. Three joins are used and target a {@link SliceOp}.
     * The 2nd and 3rd joins are in embedded an {@link SubqueryOp}. The
     * optional join group contains a filter that uses a variable outside the
     * optional join group.
     * <P>
     * The query takes the form:
     * 
     * <pre>
     * (a b)
     * optional { 
     *   (b c) 
     *   (c d) 
     *   filter(a != Paul) 
     * }
     * </pre>
     * 
     * The (a b) tail will match everything in the knowledge base. The join
     * group takes us two hops out from ?b. There should be two solutions that
     * succeed the optional join group:
     * 
     * <pre>
     * (john mary brad fred)
     * (john mary brad leon)
     * </pre>
     * 
     * and six more that don't succeed the optional join group:
     * 
     * <pre>
     * (paul mary) *
     * (paul brad) *
     * (john brad)
     * (mary brad)
     * (brad fred)
     * (brad leon)
     * </pre>
     * 
     * In the cases marked with a <code>*</code>, ?a is bound to Paul even
     * though there is a filter that specifically prohibits a = Paul. This is
     * because the filter is inside the optional join group, which means that
     * solutions can still include a = Paul, but the optional join group should
     * not run in that case.
     */
    public void test_query_optionals_filter2() throws Exception {

        // main query
        final int startId = 1;
        final int joinId1 = 2;
        final int predId1 = 3; // (a,b)
        final int condId = 4;  // (a != Paul)
        final int joinGroup1 = 10;
        final int sliceId = 9;
        
        // subquery (iff condition is satisfied)
        final int joinId2 = 5; //             : group1
        final int predId2 = 6; // (b,c)
        final int joinId3 = 7; //             : group1
        final int predId3 = 8; // (c,d)
        
        final IVariable<?> a = Var.var("a");
        final IVariable<?> b = Var.var("b");
        final IVariable<?> c = Var.var("c");
        final IVariable<?> d = Var.var("d");

//        final Integer joinGroup1 = Integer.valueOf(1);

        /*
         * Not quite sure how to write this one.  I think it probably goes
         * something like this:
         * 
         * 1. startOp
         * 2. join1Op(a b)
         * 3. conditionalRoutingOp( if a = Paul then goto sliceOp )
         * 4. join2Op(b c)
         * 5. join3Op(c d)
         * 6. sliceOp
         */
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(
                new IVariableOrConstant[] { a, b }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(
                new IVariableOrConstant[] { b, c }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred3Op = new Predicate<E>(
                new IVariableOrConstant[] { c, d }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId3),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final PipelineOp join1Op = new PipelineJoin<E>(//
                new BOp[]{startOp},// 
                        new NV(Predicate.Annotations.BOP_ID, joinId1),//
                        new NV(PipelineJoin.Annotations.PREDICATE,pred1Op));

        final IConstraint condition = Constraint.wrap(new NEConstant(a, new Constant<String>("Paul")));
        
        final ConditionalRoutingOp condOp = new ConditionalRoutingOp(new BOp[]{join1Op},
                NV.asMap(new NV[]{//
                    new NV(BOp.Annotations.BOP_ID,condId),
                    new NV(PipelineOp.Annotations.SINK_REF, joinGroup1), // a != Paul
                    new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId), // a == Paul
                    new NV(ConditionalRoutingOp.Annotations.CONDITION, condition),
                }));

        final PipelineOp subQuery;
        {
		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { /*condOp*/ },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op)//
//				// join is optional.
//				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
//				// optional target is the same as the default target.
//				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
				);

		final PipelineOp join3Op = new PipelineJoin<E>(//
				new BOp[] { join2Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId3),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred3Op)//
//				// join is optional.
//				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
//				// optional target is the same as the default target.
//				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
				);
        subQuery = join3Op;
        }
        
        final PipelineOp joinGroup1Op = new SubqueryOp(new BOp[]{condOp}, 
                new NV(Predicate.Annotations.BOP_ID, joinGroup1),//
//                new NV(PipelineOp.Annotations.CONDITIONAL_GROUP, joinGroup1),//
                new NV(SubqueryOp.Annotations.SUBQUERY, subQuery),//
//                new NV(BOp.Annotations.CONTROLLER,true)//
//                new NV(BOp.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.CONTROLLER)//
                // join is optional.
                new NV(SubqueryOp.Annotations.OPTIONAL, true)//
//                // optional target is the same as the default target.
//                new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId)
        );
        
        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{joinGroup1Op},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        }));

        final PipelineOp query = sliceOp;

        // start the query.
        final UUID queryId = UUID.randomUUID();
        final IChunkMessage<IBindingSet> initialChunkMessage;
        {

            final IBindingSet initialBindings = new HashBindingSet();

//            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
                    queryId, startId,//
                    -1, // partitionId
                    newBindingSetIterator(initialBindings));
        }
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialChunkMessage);

        // verify solutions.
        {

            // the expected solutions.
            final IBindingSet[] expected = new IBindingSet[] {//
            // two solutions where the optional join succeeds.
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Leon") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b, c, d },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Mary"),
                            new Constant<String>("Brad"),
                    		new Constant<String>("Fred") }//
            ),
            // plus anything we read from the first access path which did not 
            // pass the optional join
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Mary") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("John"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("Brad") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Brad"),
                            new Constant<String>("Fred") }//
            ),
            new ArrayBindingSet(//
                    new IVariable[] { a, b },//
                    new IConstant[] { new Constant<String>("Brad"),
                            new Constant<String>("Leon") }//
            )
            };

            assertSameSolutionsAnyOrder(expected,
                    new Dechunkerator<IBindingSet>(runningQuery.iterator()));
        
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(5, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

    }

}
