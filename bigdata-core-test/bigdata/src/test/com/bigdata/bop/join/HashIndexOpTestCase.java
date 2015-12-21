package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.AbstractHashJoinUtilityTestCase.JoinSetup;
import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.htree.HTree;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.stream.Stream.StreamIndexMetadata;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract test suite for {@link HashIndexOp} implementations.
 * 
 * TODO Test variant with non-empty join vars.
 * 
 * TODO Test variant with SELECT projects only the selected variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class HashIndexOpTestCase extends TestCase2 {

    public HashIndexOpTestCase() {
    }

    public HashIndexOpTestCase(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.MemStore.toString());

        return p;

    }

    private Journal jnl;

    protected QueryEngine queryEngine;

    public void setUp() throws Exception {

        jnl = new Journal(getProperties());

        queryEngine = new QueryEngine(jnl);

        queryEngine.init();

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
     * Factory for the {@link HashIndexOp} implementations.
     * 
     * @param namespace
     *            The namespace of the lexicon relation (required by the ivCache
     *            for the {@link HTree} variants of the {@link HashIndexOp}).
     */
    abstract protected HashIndexOp newHashIndexOp(final String namespace,
            final BOp[] args, final NV... anns);

    /**
     * Factory for the {@link SolutionSetHashJoinOp} implementation.
     */
    abstract protected SolutionSetHashJoinOp newSolutionSetHashJoinOp(
            final BOp[] args, final NV... anns);

    /**
     * Combines the two arrays, appending the contents of the 2nd array to the
     * contents of the first array.
     * 
     * @param a
     * @param b
     * @return
     */
    @SuppressWarnings("unchecked")
    protected static <T> T[] concat(final T[] a, final T[] b) {

        if (a == null && b == null)
            return a;

        if (a == null)
            return b;

        if (b == null)
            return a;

        final T[] c = (T[]) java.lang.reflect.Array.newInstance(a.getClass()
                .getComponentType(), a.length + b.length);

        // final String[] c = new String[a.length + b.length];

        System.arraycopy(a, 0, c, 0, a.length);

        System.arraycopy(b, 0, c, a.length, b.length);

        return c;

    }

    /**
     * A simple test of a {@link HashIndexOp} followed by a
     * {@link SolutionSetHashJoinOp}. In practice we should never follow the
     * {@link HashIndexOp} immediately with a {@link SolutionSetHashJoinOp} as
     * this is basically a complex NOP. However, this does provide a simple test
     * of the most basic mechanisms for those two operators.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashIndexOp_01() throws Exception {

        final JoinSetup setup = new JoinSetup(getName());
        
        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        final IVariable[] joinVars = new IVariable[]{};
        
        final IVariable[] selectVars = null;
        
        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(queryId, solutionSetName, joinVars);

        final HashIndexOp op = newHashIndexOp(setup.namespace, BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.SELECT, selectVars),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet),//
                new NV(IPredicate.Annotations.RELATION_NAME, "kb")
        );

        final SolutionSetHashJoinOp op2 = newSolutionSetHashJoinOp(
                new BOp[] { op },//
                new NV(BOp.Annotations.BOP_ID, 2),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                new NV(SolutionSetHashJoinOp.Annotations.OPTIONAL, op.isOptional()),//
//                new NV(SolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                new NV(SolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
                new NV(SolutionSetHashJoinOp.Annotations.RELEASE, true),//
                new NV(SolutionSetHashJoinOp.Annotations.LAST_PASS, true),//
                new NV(SolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        final PipelineOp query = op2;

        // The source solutions.
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IBindingSet[] bindingSets1 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.leon));
            bindingSets1[0] = tmp;
        }
        
        final IBindingSet[] bindingSets2 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            tmp.set(y, new Constant<IV>(setup.john));
            bindingSets2[0] = tmp;
        }
        
        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
            new IVariable[] { x },//
            new IConstant[] { new Constant<IV>(setup.leon) }//
            ), //
        new ListBindingSet(//
            new IVariable[] { x, y },//
            new IConstant[] { new Constant<IV>(setup.mary), 
                new Constant<IV>(setup.john) }//
        ),//
        };

        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                null/* queryAttributes */, concat(bindingSets1, bindingSets2));

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);
        
    }

    /**
     * Unit test of variant with an OPTIONAL join.
     * <p>
     * Note: Since there are no intervening joins or filters, this produces the
     * same output as the unit test above. However, in this case the joinSet
     * will have been created by the {@link HashIndexOp} and utilized by the
     * {@link SolutionSetHashJoinOp}.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashIndexOp_02() throws Exception {

        final JoinSetup setup = new JoinSetup(getName());
        
        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        final IVariable[] joinVars = new IVariable[]{};
        
        final IVariable[] selectVars = null;
        
        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(queryId, solutionSetName, joinVars);

        final HashIndexOp op = newHashIndexOp(setup.namespace,BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Optional),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.SELECT, selectVars),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet),//
                new NV(IPredicate.Annotations.RELATION_NAME, "kb")
        );

        final SolutionSetHashJoinOp op2 = newSolutionSetHashJoinOp(
                new BOp[] { op },//
                new NV(BOp.Annotations.BOP_ID, 2),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                new NV(SolutionSetHashJoinOp.Annotations.OPTIONAL, op.isOptional()),//
//                new NV(SolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                new NV(SolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
                new NV(SolutionSetHashJoinOp.Annotations.RELEASE, true),//
                new NV(SolutionSetHashJoinOp.Annotations.LAST_PASS, true),//
                new NV(SolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        final PipelineOp query = op2;

        // The source solutions.
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IBindingSet[] bindingSets1 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.leon));
            bindingSets1[0] = tmp;
        }
        
        final IBindingSet[] bindingSets2 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            tmp.set(y, new Constant<IV>(setup.john));
            bindingSets2[0] = tmp;
        }
        
        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
            new IVariable[] { x },//
            new IConstant[] { new Constant<IV>(setup.leon) }//
            ), //
        new ListBindingSet(//
            new IVariable[] { x, y },//
            new IConstant[] { new Constant<IV>(setup.mary), 
                new Constant<IV>(setup.john) }//
        ),//
        };

        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                null/* queryAttributes */, concat(bindingSets1, bindingSets2));

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);
        
    }

    /**
     * Test variant where the index is built from a {@link SolutionSetStream}
     * available as an attribute to the {@link IRunningQuery}.
     * 
     * FIXME Also test with {@link IHashJoinUtility} query attribute as the
     * <em>source</em> (rather than a {@link SolutionSetStream}) and an
     * {@link IPredicate}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_hashIndexOp_buildFromSolutionSet() throws Exception {

        final JoinSetup setup = new JoinSetup(getName());
        
        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        final IVariable[] joinVars = new IVariable[]{};
        
        final IVariable[] selectVars = null;
        
        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(queryId, solutionSetName, joinVars);

        final INamedSolutionSetRef namedSolutionSetSource = NamedSolutionSetRefUtility
                .newInstance(queryId, solutionSetName + "source", joinVars);

        // The Stream will be attached to the query attributes
        final Map<Object, Object> queryAttributes = new LinkedHashMap<Object, Object>();

        // The solutions to index.
        final IBindingSet[] solutionsToIndex;
//        solutionsToIndex = setup.getRight1().toArray(
//                new IBindingSet[setup.getRight1().size()]);
        solutionsToIndex = setup.getLeft1().toArray(
                new IBindingSet[setup.getLeft1().size()]);
        
        final SolutionSetStream stream;
        {

            final StreamIndexMetadata metadata = new StreamIndexMetadata(
                    UUID.randomUUID());

            // Create stream.
            stream = SolutionSetStream.create(jnl, metadata);

            /*
             * Populate the stream.
             */
            {
                IStriterator itr = new Striterator(Arrays.asList(
                        solutionsToIndex).iterator());

                // wrap each bindingSet as an IBindingSet[].
                itr.addFilter(new Resolver(){

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Object resolve(Object obj) {

                        return new IBindingSet[] { (IBindingSet) obj };
                        
                    }});
                
                stream.put((ICloseableIterator<IBindingSet[]>) itr);
                
            }

            // Checkpoint.
            stream.writeCheckpoint2();
            
            // Attach to query attributes (make visible to the query).
            queryAttributes.put(namedSolutionSetSource, stream);
            
        }
        
        // Add operator to build the hash index.
        final HashIndexOp op = newHashIndexOp(setup.namespace,BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Optional),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.SELECT, selectVars),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet),//
                new NV(HashIndexOp.Annotations.NAMED_SET_SOURCE_REF, namedSolutionSetSource),//
                new NV(IPredicate.Annotations.RELATION_NAME, "kb")
        );

        // Add operator to validate the hash index build.
        final ValidateIndexOp op2 = new ValidateIndexOp(
                new BOp[] { op },
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, 2),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
//                        new NV(PipelineOp.Annotations.LAST_PASS, true),//
//                        new NV(PipelineOp.Annotations.MAX_PARALLEL,1),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(ValidateIndexOp.Annotations.EXPECTED_SOLUTIONS,
                               solutionsToIndex),//
                        new NV(ValidateIndexOp.Annotations.NAMED_SET_SOURCE_REF,
                               namedSolutionSetSource),//
                        new NV(ValidateIndexOp.Annotations.NAMED_SET_REF,
                               namedSolutionSet),//
                }));

        final PipelineOp query = op2;

        // The source solutions.
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IBindingSet[] bindingSets1 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.leon));
            bindingSets1[0] = tmp;
        }
        
        final IBindingSet[] bindingSets2 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            tmp.set(y, new Constant<IV>(setup.john));
            bindingSets2[0] = tmp;
        }
        
        // the source solutions fed into the query.
        final IBindingSet[] bindingSets = concat(bindingSets1, bindingSets2);

        // the expected solutions (same as the source solutions).
        final IBindingSet[] expected = bindingSets;

        // run the query. it will build the index as a side-effect.
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                queryAttributes, bindingSets);

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);

    }

    /**
     * Operator is used to validate the {@link HashIndexOp} by verifying the
     * contents of the generated index before the life cycle of the index is
     * ended (when the {@link IRunningQuery} ends, the memory manager associated
     * with the query is cleared and the index data will no longer be valid).
     */
    private static class ValidateIndexOp extends PipelineOp {

        private static final long serialVersionUID = 1L;

        interface Annotations extends PipelineOp.Annotations {
            
            /**
             * Identifies the solutions that should have been indexed by the
             * {@link HashIndexOp}.
             */
            String NAMED_SET_SOURCE_REF = HashIndexOp.Annotations.NAMED_SET_SOURCE_REF;

            /**
             * Identifies the index that is the output of the
             * {@link HashIndexOp}. This is the index that we will validate.
             */
            String NAMED_SET_REF = HashIndexOp.Annotations.NAMED_SET_REF;

            /**
             * The expected solutions that should be reported by a scan of the
             * generated index.
             */
            String EXPECTED_SOLUTIONS = ValidateIndexOp.class.getName()
                    + ".expectedSolutions";

        }
        
        public ValidateIndexOp(final BOp[] args,
                final Map<String, Object> annotations) {

            super(args, annotations);
            
            /*
             * This is not strictly necessary, but it does simplify validation
             * if we can assume that there will be exactly one invocation of
             * this operator.
             */

            assertTrue(isAtOnceEvaluation());
            
        }

        public ValidateIndexOp(ValidateIndexOp op) {
            super(op);
        }

        @Override
        public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

            return new FutureTask<Void>(new ChunkTask(this, context));

        }

        private static class ChunkTask implements Callable<Void> {

            private final ValidateIndexOp op;
            private final BOpContext<IBindingSet> context;

            public ChunkTask(final ValidateIndexOp op,
                    final BOpContext<IBindingSet> context) {

                this.op = op;

                this.context = context;

            }

            /**
             * We need to verify that the named solution set was created and
             * attached to the query and that the correct solutions were placed
             * into the index.
             */
            @Override
            public Void call() throws Exception {

                // The solutions that should be in the generated index.
                final IBindingSet[] expectedSolutions = (IBindingSet[]) op
                        .getRequiredProperty(Annotations.EXPECTED_SOLUTIONS);
                
                // Metadata to identify the generated solution set.
                final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_REF);

                // Metadata to identify the source solution set.
                final INamedSolutionSetRef namedSetSourceRef = (INamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_SOURCE_REF);

                // The actual query attributes from the query.
                final IQueryAttributes queryAttributes2 = context
                        .getQueryAttributes();

                // verify that the generated solution set exists.
                assertNotNull("Generated index not found: " + namedSetRef,
                        queryAttributes2.get(namedSetRef));

                // verify that the source solution set exists.
                assertNotNull("Source not found: " + namedSetSourceRef,
                        queryAttributes2.get(namedSetSourceRef));

                /*
                 * Verify that the generated index has the correct solutions.
                 */
                final IHashJoinUtility tmp = (IHashJoinUtility) queryAttributes2
                        .get(namedSetRef);

                // Verify index scan against expected solutions.
                AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(
                        expectedSolutions, tmp.indexScan());

                /*
                 * Copy the source solutions to the sink.
                 */
                BOpUtility.copy(context.getSource(), context.getSink(),
                        null/* sink2 */, null/* mergeSolution */,
                        null/* selectVars */, null/* constraints */,
                        context.getStats());

                // Flush the sink.
                context.getSink().flush();

                return null;

            }

        }

    }

}
