package com.bigdata.bop.aggregation;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * A pipelined DISTINCT operator based on a hash table.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class DistinctBindingSetOp extends AbstractPipelineOp<IBindingSet>{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations {

        /**
         * The initial capacity of the {@link ConcurrentHashMap} used to impose
         * the distinct constraint.
         * 
         * @see #DEFAULT_INITIAL_CAPACITY
         */
        String INITIAL_CAPACITY = "initialCapacity";

        int DEFAULT_INITIAL_CAPACITY = 16;

        /**
         * The load factor of the {@link ConcurrentHashMap} used to impose
         * the distinct constraint.
         * 
         * @see #DEFAULT_LOAD_FACTOR
         */
        String LOAD_FACTOR = "loadFactor";

        float DEFAULT_LOAD_FACTOR = .75f;

        /**
         * The concurrency level of the {@link ConcurrentHashMap} used to impose
         * the distinct constraint.
         * 
         * @see #DEFAULT_CONCURRENCY_LEVEL
         */
        String CONCURRENCY_LEVEL = "concurrencyLevel";

        int DEFAULT_CONCURRENCY_LEVEL = 16;
        
        /**
         * The variables on which the distinct constraint will be imposed.
         * Binding sets with distinct values for the specified variables will be
         * passed on.
         */
        String VARIABLES = DistinctBindingSetOp.class.getName() + ".variables";
        
    }

    /**
     * Required deep copy constructor.
     */
    public DistinctBindingSetOp(final DistinctBindingSetOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public DistinctBindingSetOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        final IVariable<?>[] vars = getVariables();

        if (vars == null)
            throw new IllegalArgumentException();

        if (vars.length == 0)
            throw new IllegalArgumentException();

    }

    /**
     * @see Annotations#INITIAL_CAPACITY
     */
    public int getInitialCapacity() {

        return getProperty(Annotations.INITIAL_CAPACITY,
                Annotations.DEFAULT_INITIAL_CAPACITY);

    }

    /**
     * @see Annotations#LOAD_FACTOR
     */
    public float getLoadFactor() {

        return getProperty(Annotations.LOAD_FACTOR,
                Annotations.DEFAULT_LOAD_FACTOR);

    }

    /**
     * @see Annotations#CONCURRENCY_LEVEL
     */
    public int getConcurrencyLevel() {

        return getProperty(Annotations.CONCURRENCY_LEVEL,
                Annotations.DEFAULT_CONCURRENCY_LEVEL);

    }
    
    /**
     * @see Annotations#VARIABLES
     */
    public IVariable<?>[] getVariables() {

        return (IVariable<?>[]) annotations.get(Annotations.VARIABLES);
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new DistinctTask(this, context));
        
    }

    /**
     * Wrapper used for the as bound solutions in the {@link ConcurrentHashMap}.
     */
    private static class Solution {
        private final int hash;

        private final IConstant<?>[] vals;

        public Solution(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Solution)) {
                return false;
            }
            final Solution t = (Solution) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                // @todo allow for nulls.
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }
    }
    
    /**
     * Task executing on the node.
     */
    private class DistinctTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * A concurrent map whose keys are the bindings on the specified
         * variables (the keys and the values are the same since the map
         * implementation does not allow <code>null</code> values).
         */
        private /*final*/ ConcurrentHashMap<Solution, Solution> map;

        /**
         * The variables used to impose a distinct constraint.
         */
        private final IVariable<?>[] vars;
        
        DistinctTask(final DistinctBindingSetOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.vars = op.getVariables();

            this.map = new ConcurrentHashMap<Solution, Solution>(
                    getInitialCapacity(), getLoadFactor(),
                    getConcurrencyLevel());

        }

        /**
         * If the bindings are distinct for the configured variables then return
         * those bindings.
         * 
         * @param bset
         *            The binding set to be filtered.
         * 
         * @return The distinct as bound values -or- <code>null</code> if the
         *         binding set duplicates a solution which was already accepted.
         */
        private IConstant<?>[] accept(final IBindingSet bset) {

            final IConstant<?>[] r = new IConstant<?>[vars.length];

            for (int i = 0; i < vars.length; i++) {

                if ((r[i] = bset.get(vars[i])) == null) {

                    /*
                     * @todo probably allow nulls, but write a unit test for it.
                     */

                    throw new RuntimeException("Not bound: " + vars[i]);

                }

            }

            final Solution s = new Solution(r);
            
            final boolean distinct = map.putIfAbsent(s, s) == null;

            return distinct ? r : null;

        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                while (itr.hasNext()) {
                    
                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    final List<IBindingSet> accepted = new LinkedList<IBindingSet>();

                    int naccepted = 0;

                    for (IBindingSet bset : a) {

//                        System.err.println("considering: " + bset);

                        final IConstant<?>[] vals = accept(bset);

                        if (vals != null) {

//                            System.err.println("accepted: "
//                                    + Arrays.toString(vals));

                            /*
                             * @todo This may cause problems since the
                             * ArrayBindingSet does not allow mutation with
                             * variables not declared up front. In that case use
                             * new HashBindingSet( new ArrayBindingSet(...)).
                             */
                            
                            accepted.add(new ArrayBindingSet(vars, vals));

                            naccepted++;

                        }

                    }

                    if (naccepted > 0) {

                        final IBindingSet[] b = accepted
                                .toArray(new IBindingSet[naccepted]);
                        
//                        System.err.println("output: "
//                                + Arrays.toString(b));

                        sink.add(b);

                        stats.unitsOut.add(naccepted);
                        stats.chunksOut.increment();

                    }

                }

                // done.
                return null;
                
            } finally {

                sink.flush();
                sink.close();

                // discard the map.
                map = null;

            }

        }

    }

}
