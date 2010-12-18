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

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase2;

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
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for handling of optional join groups during query evaluation
 * against a local database instance. Optional join groups are handled using
 * {@link IBindingSet#push()} when entering the join group and
 * {@link IBindingSet#pop(boolean)} when exiting the join group. If the join
 * group was successful for a given binding set, then <code>save:=true</code> is
 * specified for {@link IBindingSet#pop(boolean)} and the applied bindings will
 * be visible to the downstream consumer. Otherwise the bindings applied during
 * the join group are simply discarded.
 * 
 * <pre>
 * -Dlog4j.configuration=bigdata/src/resources/logging/log4j.properties
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestQueryEngine.java 3950 2010-11-17 02:14:08Z thompsonbry $
 */
public class TestQueryEngineOptionalJoins extends TestCase2 {

    /**
     * 
     */
    public TestQueryEngineOptionalJoins() {
    }

    /**
     * @param name
     */
    public TestQueryEngineOptionalJoins(String name) {
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
    Journal jnl;
    QueryEngine queryEngine;
    
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
                
                new E("John", "Mary"),// [0]
                new E("John", "Brad"),// [1]
                
                new E("Mary", "Brad"),// [1]
                
                new E("Brad", "Fred"),// [1]
                new E("Brad", "Leon"),// [1]
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
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSets
     *            the binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[] bindingSets) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { bindingSets });

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSetChunks
     *            the chunks of binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSetChunks) {

        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSetChunks);

    }

    /**
     * Unit test for optional join group. Three joins are used and target a
     * {@link SliceOp}. The 2nd and 3rd joins are an optional join group. 
     * Intermediate results which do not succeed on the optional join are 
     * forwarded to the {@link SliceOp} which is the target specified by the
     * {@link PipelineOp.Annotations#ALT_SINK_REF}.
     * 
     * The optional join group takes the form:
     * (a b)
     * optional { 
     *   (b c) 
     *   (c d) 
     * }
     *
     * The (a b) tail will match everything in the knowledge base.  The join
     * group takes us two hops out from ?b.  There should be four solutions
     * that succeed the optional join group:
     * 
     * (paul mary brad fred)
     * (paul mary brad leon)
     * (john mary brad fred)
     * (john mary brad leon)
     * 
     * and five more that don't succeed the optional join group:
     * 
     * (paul brad) *
     * (john brad) *
     * (mary brad) *
     * (brad fred)
     * (brad leon)
     * 
     * In this cases marked with a *, ?c will become temporarily bound to fred 
     * and leon (since brad knows fred and leon), but the (c d) tail will fail 
     * since fred and leon don't know anyone else. At this point, the ?c binding
     * must be removed from the solution.
     */
    public void test_query_join2_optionals() throws Exception {

        final int startId = 1; // 
        final int joinId1 = 2; //         : base join group.
        final int predId1 = 3; // (a b)
        final int joinId2 = 4; //         : joinGroup1
        final int predId2 = 5; // (b c)   
        final int joinId3 = 6; //         : joinGroup1
        final int predId3 = 7; // (c d)
        final int sliceId = 8; // 
        
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

		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { join1Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op),//
				// join is optional.
				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

		final PipelineOp join3Op = new PipelineJoin<E>(//
				new BOp[] { join2Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId3),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred3Op),//
				// join is optional.
				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{join3Op},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
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
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
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

    /**
     * Unit test for optional join group with a filter. Three joins are used 
     * and target a {@link SliceOp}. The 2nd and 3rd joins are an optional join 
     * group. Intermediate results which do not succeed on the optional join are 
     * forwarded to the {@link SliceOp} which is the target specified by the
     * {@link PipelineOp.Annotations#ALT_SINK_REF}.  The optional join group
     * contains a filter.
     * 
     * The optional join group takes the form:
     * (a b)
     * optional { 
     *   (b c) 
     *   (c d) 
     *   filter(d != Leon) 
     * }
     *
     * The (a b) tail will match everything in the knowledge base.  The join
     * group takes us two hops out from ?b.  There should be two solutions
     * that succeed the optional join group:
     * 
     * (paul mary brad fred)
     * (john mary brad fred)
     * 
     * and five more that don't succeed the optional join group:
     * 
     * (paul brad) *
     * (john brad) *
     * (mary brad) *
     * (brad fred)
     * (brad leon)
     * 
     * In this cases marked with a *, ?c will become temporarily bound to fred 
     * and leon (since brad knows fred and leon), but the (c d) tail will fail 
     * since fred and leon don't know anyone else. At this point, the ?c binding
     * must be removed from the solution.
     *
     * The filter (d != Leon) will prune the two solutions:
     * 
     * (paul mary brad leon)
     * (john mary brad leon)
     * 
     * since ?d is bound to Leon in those cases.
     */
    public void test_query_optionals_filter() throws Exception {

        final int startId = 1;
        final int joinId1 = 2;
        final int predId1 = 3;
        final int joinId2 = 4;
        final int predId2 = 5;
        final int joinId3 = 6;
        final int predId3 = 7;
        final int sliceId = 8;
        
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

		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { join1Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op),//
				// join is optional.
				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

		final PipelineOp join3Op = new PipelineJoin<E>(//
				new BOp[] { join2Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId3),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred3Op),//
				// constraint d != Leon
				new NV(PipelineJoin.Annotations.CONSTRAINTS,
						new IConstraint[] { new NEConstant(d, new Constant<String>("Leon")) }),
				// join is optional.
				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{join3Op},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
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
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
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
            assertEquals(5, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

    }

    /**
     * Unit test for optional join group with a filter on a variable outside 
     * the optional join group. Three joins are used and target a 
     * {@link SliceOp}. The 2nd and 3rd joins are an optional join 
     * group. Intermediate results which do not succeed on the optional join are 
     * forwarded to the {@link SliceOp} which is the target specified by the
     * {@link PipelineOp.Annotations#ALT_SINK_REF}.  The optional join group
     * contains a filter that uses a variable outside the optional join group.
     * 
     * The query takes the form:
     * (a b)
     * optional { 
     *   (b c) 
     *   (c d) 
     *   filter(a != Paul) 
     * }
     *
     * The (a b) tail will match everything in the knowledge base.  The join
     * group takes us two hops out from ?b.  There should be two solutions
     * that succeed the optional join group:
     * 
     * (john mary brad fred)
     * (john mary brad leon)
     * 
     * and six more that don't succeed the optional join group:
     *
     * (paul mary) *
     * (paul brad) *
     * (john brad)
     * (mary brad)
     * (brad fred)
     * (brad leon)
     * 
     * In this cases marked with a *, ?a is bound to Paul even though there is
     * a filter that specifically prohibits a = Paul. This is because the filter
     * is inside the optional join group, which means that solutions can still
     * include a = Paul, but the optional join group should not run in that
     * case.
     */
    public void test_query_optionals_filter2() throws Exception {

        final int startId = 1;
        final int joinId1 = 2;
        final int predId1 = 3;
        final int condId = 4;
        final int joinId2 = 5;
        final int predId2 = 6;
        final int joinId3 = 7;
        final int predId3 = 8;
        final int sliceId = 9;
        
        final IVariable<?> a = Var.var("a");
        final IVariable<?> b = Var.var("b");
        final IVariable<?> c = Var.var("c");
        final IVariable<?> d = Var.var("d");
        
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

        final IConstraint condition = new EQConstant(a, new Constant<String>("Paul"));
        
        final ConditionalRoutingOp condOp = new ConditionalRoutingOp(new BOp[]{join1Op},
                NV.asMap(new NV[]{//
                    new NV(BOp.Annotations.BOP_ID,condId),
                    new NV(PipelineOp.Annotations.SINK_REF, joinId2),
                    new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId),
                    new NV(ConditionalRoutingOp.Annotations.CONDITION, condition),
                }));

		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { condOp },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op),//
				// join is optional.
				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

		final PipelineOp join3Op = new PipelineJoin<E>(//
				new BOp[] { join2Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId3),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred3Op),//
				// join is optional.
				new NV(PipelineJoin.Annotations.OPTIONAL, true),//
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{join3Op},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
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
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
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
                    		new Constant<String>("John") }//
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
            assertEquals(6, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

    }

    /**
     * Verify the expected solutions.
     * 
     * @param expected
     * @param itr
     */
    static public void assertSameSolutions(final IBindingSet[] expected,
            final IAsynchronousIterator<IBindingSet[]> itr) {
        try {
            int n = 0;
            while (itr.hasNext()) {
                final IBindingSet[] e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : chunkSize=" + e.length);
                for (int i = 0; i < e.length; i++) {
                    if (log.isInfoEnabled())
                        log.info(n + " : " + e[i]);
                    if (n >= expected.length) {
                        fail("Willing to deliver too many solutions: n=" + n
                                + " : " + e[i]);
                    }
                    if (!expected[n].equals(e[i])) {
                        fail("n=" + n + ", expected=" + expected[n]
                                + ", actual=" + e[i]);
                    }
                    n++;
                }
            }
            assertEquals("Wrong number of solutions", expected.length, n);
        } finally {
            itr.close();
        }
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public <T> void assertSameSolutionsAnyOrder(final T[] expected,
            final Iterator<T> actual) {

        assertSameSolutionsAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public <T> void assertSameSolutionsAnyOrder(final String msg,
            final T[] expected, final Iterator<T> actual) {

        try {

            /*
             * Populate a map that we will use to realize the match and
             * selection without replacement logic. The map uses counters to
             * handle duplicate keys. This makes it possible to write tests in
             * which two or more binding sets which are "equal" appear.
             */

            final int nrange = expected.length;

            final java.util.Map<T, AtomicInteger> range = new java.util.LinkedHashMap<T, AtomicInteger>();

            for (int j = 0; j < nrange; j++) {

                AtomicInteger count = range.get(expected[j]);

                if (count == null) {

                    count = new AtomicInteger();

                }

                range.put(expected[j], count);

                count.incrementAndGet();
                
            }

            // Do selection without replacement for the objects visited by
            // iterator.

            for (int j = 0; j < nrange; j++) {

                if (!actual.hasNext()) {

                    fail(msg
                            + ": Iterator exhausted while expecting more object(s)"
                            + ": index=" + j);

                }

                final T actualObject = actual.next();

                if (log.isInfoEnabled())
                    log.info("visting: " + actualObject);

                AtomicInteger counter = range.get(actualObject);

                if (counter == null || counter.get() == 0) {

                    fail("Object not expected" + ": index=" + j + ", object="
                            + actualObject);

                }

                counter.decrementAndGet();
                
            }

            if (actual.hasNext()) {

                fail("Iterator will deliver too many objects.");

            }

        } finally {

            if (actual instanceof ICloseableIterator<?>) {

                ((ICloseableIterator<T>) actual).close();

            }

        }

    }

}
