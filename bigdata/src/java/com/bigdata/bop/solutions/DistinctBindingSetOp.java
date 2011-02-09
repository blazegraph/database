package com.bigdata.bop.solutions;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.ConcurrentHashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * A pipelined DISTINCT operator based on a hash table.
 * <p>
 * Note: This implementation is a pipelined operator which inspects each chunk
 * of solutions as they arrive and those solutions which are distinct for each
 * chunk processed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class DistinctBindingSetOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            ConcurrentHashMapAnnotations {

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

		switch (getEvaluationContext()) {
		case CONTROLLER:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

		// shared state is used to share the hash table.
		if (isSharedState()) {
			throw new UnsupportedOperationException(Annotations.SHARED_STATE
					+ "=" + isSharedState());
		}
	
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

        return (IVariable<?>[]) getRequiredProperty(Annotations.VARIABLES);
        
    }

    public BOpStats newStats() {
    	
    	return new DistinctStats(this);
    	
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
                // @todo verify that this allows for nulls with a unit test.
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
	 * Extends {@link BOpStats} to provide the shared state for the distinct
	 * solution groups across multiple invocations of the DISTINCT operator.
	 */
    private static class DistinctStats extends BOpStats {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * A concurrent map whose keys are the bindings on the specified
		 * variables (the keys and the values are the same since the map
		 * implementation does not allow <code>null</code> values).
		 * <p>
		 * Note: The map is shared state and can not be discarded or cleared
		 * until the last invocation!!!
		 */
        private final ConcurrentHashMap<Solution, Solution> map;

    	public DistinctStats(final DistinctBindingSetOp op) {
    		
            this.map = new ConcurrentHashMap<Solution, Solution>(
                    op.getInitialCapacity(), op.getLoadFactor(),
                    op.getConcurrencyLevel());

    	}
    	
    }
    
    /**
     * Task executing on the node.
     */
    static private class DistinctTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

		/**
		 * A concurrent map whose keys are the bindings on the specified
		 * variables (the keys and the values are the same since the map
		 * implementation does not allow <code>null</code> values).
		 */
        private final ConcurrentHashMap<Solution, Solution> map;

        /**
         * The variables used to impose a distinct constraint.
         */
        private final IVariable<?>[] vars;
        
        DistinctTask(final DistinctBindingSetOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.vars = op.getVariables();

            if (vars == null)
                throw new IllegalArgumentException();

            if (vars.length == 0)
                throw new IllegalArgumentException();

			// The map is shared state across invocations of this operator task.
			this.map = ((DistinctStats) context.getStats()).map;

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

                /*
                 * Note: This allows null's.
                 * 
                 * @todo write a unit test when some variables are not bound.
                 */
                r[i] = bset.get(vars[i]);

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

                    // The distinct solutions accepted from this chunk. 
                    final List<IBindingSet> accepted = new LinkedList<IBindingSet>();

                    int naccepted = 0;

                    for (IBindingSet bset : a) {

//                        System.err.println("considering: " + bset);

						/*
						 * Test to see if this solution is distinct from those
						 * already seen.
						 */
                        final IConstant<?>[] vals = accept(bset);

                        if (vals != null) {

							/*
							 * This is a distinct solution. Copy only the
							 * variables used to select distinct solutions into
							 * a new binding set and add that to the set of
							 * [accepted] binding sets which will be emitted by
							 * this operator.
							 */
                        	
//                            System.err.println("accepted: "
//                                    + Arrays.toString(vals));

							final ListBindingSet tmp = new ListBindingSet();
                        	
							for (int i = 0; i < vars.length; i++) {

								tmp.set(vars[i], vals[i]);

							}
							
                            accepted.add(tmp);

                            naccepted++;

                        }

                    }

                    if (naccepted > 0) {

						/*
						 * At least one solution was accepted as distinct, so
						 * copy the selected solutions to the output of the
						 * operator.
						 */
                    	
                        final IBindingSet[] b = accepted
                                .toArray(new IBindingSet[naccepted]);
                        
//                        System.err.println("output: "
//                                + Arrays.toString(b));

                        // copy the distinct solutions to the output.
                        sink.add(b);

//                        stats.unitsOut.add(naccepted);
//                        stats.chunksOut.increment();

                    }

                }

                sink.flush();

                if(context.isLastInvocation()) {

					/*
					 * Discard the map.
					 * 
					 * Note: The map can not be discarded (or cleared) until the
					 * last invocation. However, we only get the benefit of the
					 * lastInvocation signal if the operator is single threaded
					 * and running on the query controller. That is not a
					 * requirement for this DISTINCT implementation, so the map
					 * is not going to be cleared until the query goes out of
					 * scope and is swept by GC.
					 */
                    map.clear();

                }
                
                // done.
                return null;
                
            } finally {

                sink.close();

            }

        }

    }

}
