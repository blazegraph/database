package com.bigdata.bop.solutions;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.join.HTreeHashJoinUtility;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * A pipelined DISTINCT operator based on the persistence capable {@link HTree}
 * suitable for very large solution sets. Only the variables which are used to
 * determine the DISTINCT solutions are projected from the operator. The
 * operator is specific to the RDF data model (it relies on encoded {@link IV}
 * s).
 * <p>
 * Note: This implementation is a single-threaded pipelined operator which
 * inspects each chunk of solutions as they arrive and those solutions which are
 * distinct for each chunk passed on.
 * <p>
 * Note: {@link PipelineOp.Annotations#MAX_MEMORY} is currently ignored by this
 * operator. This value could be used to trigger the switch to an external
 * memory DISTINCT (on a backing store) or to fail a query which attempts to put
 * too much data into the native heap. Right now, it will just keep adding data
 * on the native heap and eventually the machine will begin to swap.
 * 
 * FIXME This needs to be rewritten to use the fast and compact IV[] encoding
 * similar to {@link HTreeHashJoinUtility}. However, unlike that class, this
 * class does not need an ivCache as it only needs to just whether a solution,
 * when coded as an IV[], has been seen before. Use an int32 hash code of the
 * {@link IBindingSet} for the key. Use
 * {@link IVUtility#encode(IKeyBuilder, IV)} to encode the {@link IV}[]s from
 * the {@link IBindingSet} for the values. The code will need to scan the
 * collision bucket for the hash code of the {@link IBindingSet} to decide
 * whether a given solution is already in the {@link HTree}. It should compare
 * the encoded solutions as this can be done without any deserialization costs.
 * Like the {@link HTreeHashJoinUtility}, this class should maintain a
 * {@link LinkedHashSet} which provides a schema and indicates the order in
 * which variable bindings are represented in the {@link HTree} values, with
 * unbound variables represented by {@link TermId#NullIV}. There is a lot of
 * shared code, so it would be best to expand {@link HTreeHashJoinUtility},
 * perhaps defining an alternative acceptSolutions() method which passes along
 * the distinct solutions to a buffer provided by the caller. There would also
 * be no ivCache when configured for this purpose.
 * <p>
 * FIXME joinVars != variables and hashCode() != hashCode() when variables MAY
 * be unbound! Must test with unbound variables!!!! Just use TermId.NullIV in
 * the hashCode (or skip the variable in the hash code computation via a boolean
 * parameter to ignore unbound variables?)
 * <p>
 * FIXME Only the variables which are used to determine DISTINCT are projected
 * from the operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class DistinctBindingSetsWithHTreeOp extends PipelineOp {

//	private final static transient Logger log = Logger
//			.getLogger(DistinctBindingSetsWithHTreeOp.class);
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	public interface Annotations extends PipelineOp.Annotations,
			HTreeAnnotations, DistinctAnnotations {

        /**
         * The name of {@link IQueryAttributes} attribute under which the
         * {@link HTreeHashJoinState} for this operator is stored. The attribute
         * name includes the query UUID. The query UUID must be extracted and
         * used to lookup the {@link IRunningQuery} to which the solution set
         * was attached.
         * 
         * @see NamedSolutionSetRef
         */
        final String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;

	}

    /**
     * Required deep copy constructor.
     */
    public DistinctBindingSetsWithHTreeOp(final DistinctBindingSetsWithHTreeOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public DistinctBindingSetsWithHTreeOp(final BOp[] args,
            final Map<String, Object> annotations) {

		super(args, annotations);

		switch (getEvaluationContext()) {
        case CONTROLLER:
        case HASHED:
            break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

		if (getMaxParallel() != 1)
			throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
					+ "=" + getMaxParallel());

//		// shared state is used to share the hash table.
//		if (!isSharedState()) {
//			throw new UnsupportedOperationException(Annotations.SHARED_STATE
//					+ "=" + isSharedState());
//		}

        final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) getRequiredProperty(Annotations.NAMED_SET_REF);

		final IVariable<?>[] vars = (IVariable[]) getProperty(Annotations.VARIABLES);

		if (vars == null || vars.length == 0)
			throw new IllegalArgumentException();

    }

    public DistinctBindingSetsWithHTreeOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

//    /**
//     * @see Annotations#ADDRESS_BITS
//     */
//    public int getAddressBits() {
//
//		return getProperty(Annotations.ADDRESS_BITS,
//				Annotations.DEFAULT_ADDRESS_BITS);
//
//    }
//
//	/**
//	 * @see Annotations#RAW_RECORDS
//	 */
//	public boolean getRawRecords() {
//
//		return getProperty(Annotations.RAW_RECORDS,
//				Annotations.DEFAULT_RAW_RECORDS);
//
//	}
//	
//	/**
//	 * @see Annotations#MAX_RECLEN
//	 */
//	public int getMaxRecLen() {
//
//		return getProperty(Annotations.MAX_RECLEN,
//				Annotations.DEFAULT_MAX_RECLEN);
//
//	}
//
//    /**
//     * @see Annotations#VARIABLES
//     */
//    public IVariable<?>[] getVariables() {
//
//        return (IVariable<?>[]) getRequiredProperty(Annotations.VARIABLES);
//        
//    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new DistinctTask(this, context));
        
    }

//    /**
//     * Wrapper used for as bound solutions in the {@link HTree}.
//     * <p>
//     * Note: A similar class appears in different operators which use the
//     * {@link HTree}. However, these classes differ in what bindings are
//     * conceptually part of the key in the {@link HTree}, in how they compute
//     * the hash code under which the solution will be indexed, and in how they
//     * compare the solutions for equality.
//     * <p>
//     * This implementation relies on an ordered {@link IVariable}[] which
//     * defines the bindings that are part of the key and permits a simpler
//     * {@link #equals(Object)}s method that would be used if an entire
//     * {@link IBindingSet} was being tested for equality (binding sets do not
//     * consider order of the bindings when testing for equality).
//     */
//    private static class Solution implements Serializable {
//        
//        private static final long serialVersionUID = 1L;
//
//        private final int hash;
//
//        private final IConstant<?>[] vals;
//
//        /**
//         * Solution whose hash code is the hash code of the {@link IConstant}[].
//         * 
//         * @param vals
//         *            The values.
//         */
//        public Solution(final IConstant<?>[] vals) {
//
//            this.vals = vals;
//            
//            this.hash = java.util.Arrays.hashCode(vals);
//            
//        }
//
//        public int hashCode() {
//
//            return hash;
//            
//        }
//
//        public boolean equals(final Object o) {
//            if (this == o)
//                return true;
//            if (!(o instanceof Solution)) {
//                return false;
//            }
//            final Solution t = (Solution) o;
//            if (vals.length != t.vals.length)
//                return false;
//            for (int i = 0; i < vals.length; i++) {
//                if (vals[i] == t.vals[i])
//                    continue;
//                if (vals[i] == null)
//                    return false;
//                if (!vals[i].equals(t.vals[i]))
//                    return false;
//            }
//            return true;
//        }
//
//    } // class Solution

    /**
     * Task executing on the node.
     */
    static private class DistinctTask implements Callable<Void> {

        private final DistinctBindingSetsWithHTreeOp op;
        
        private final BOpContext<IBindingSet> context;

        private final HTreeHashJoinUtility state;

//        /**
//         * The variables used to impose a distinct constraint.
//         */
//        private final IVariable<?>[] vars;
        
        DistinctTask(final DistinctBindingSetsWithHTreeOp op,
                final BOpContext<IBindingSet> context) {
            
            this.op = op;
            
            this.context = context;

//            this.vars = op.getVariables();
//
//            if (vars == null)
//                throw new IllegalArgumentException();
//
//            if (vars.length == 0)
//                throw new IllegalArgumentException();
//
//			// The map is shared state across invocations of this operator task.
//			this.map = getMap(op);

            /** Metadata to identify the named solution set. */
            final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);
            
            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 */
                
                // Lookup the attributes for the query on which we will hang the
                // solution set.
                final IQueryAttributes attrs = context
                        .getQueryAttributes(namedSetRef.queryId);

                HTreeHashJoinUtility state = (HTreeHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {
                    
                    state = new HTreeHashJoinUtility(
                            context.getMemoryManager(namedSetRef.queryId), op,
                            false/*optional*/, true/* filter */);

                    if (attrs.putIfAbsent(namedSetRef, state) != null)
                        throw new AssertionError();
                                        
                }
                
                this.state = state;

            }
            
        }
        
//        /**
//         * If the bindings are distinct for the configured variables then return
//         * those bindings.
//         * 
//         * @param bset
//         *            The binding set to be filtered.
//         * 
//         * @return The distinct as bound values -or- <code>null</code> if the
//         *         binding set duplicates a solution which was already accepted.
//         */
//        private IConstant<?>[] accept(final IBindingSet bset) {
//
//			/*
//			 * Create a subset of the variable bindings which corresponds to
//			 * those which are the key for the DISTINCT operator and wrap them
//			 * as a Solution.
//			 */
//
//        	final IConstant<?>[] r = new IConstant<?>[vars.length];
//
//            for (int i = 0; i < vars.length; i++) {
//
//                /*
//                 * Note: This allows null's.
//                 * 
//                 * todo write a unit test when some variables are not bound.
//                 */
//                r[i] = bset.get(vars[i]);
//
//            }
//
//			final Solution s = new Solution(r);
//
//			if (log.isTraceEnabled())
//				log.trace("considering: " + Arrays.toString(r));
//
//			/*
//			 * Conditional insert on the map. The solution is distinct (for the
//			 * selected variables) iff the map is modified by the conditional
//			 * insert.
//			 */
//			
//			final boolean modified = insertIfAbsent(s);
//			
//			if (modified && log.isDebugEnabled())
//				log.debug("accepted: " + Arrays.toString(r));
//
//			return modified ? r : null;
//
//        }
//
//		/**
//		 * Insert the solution into the map iff there is no entry for that
//		 * solution.
//		 * <p>
//		 * Note: This has a signature similar to "putIfAbsent" on a concurrent
//		 * map, but the contract for the {@link HTree} is single threaded under
//		 * mutation so this method does NOT support concurrent updates. Instead,
//		 * it provides a simplified pattern for a conditional insert.
//		 * 
//		 * @param key
//		 *            The key.
//		 * @param val
//		 *            The value.
//		 * 
//		 * @return <code>true</code> iff the index was modified.
//		 */
//        private boolean insertIfAbsent(final Solution s) {
//
//        	final int key = s.hashCode();
//        	
//    		final ITupleIterator<Solution> titr = map.lookupAll(key);
//    		
//    		while(titr.hasNext()) {
//
//    			final ITuple<Solution> t = titr.next();
//
//    			final Solution tmp = t.getObject();
//    			
//    			if(s.equals(tmp)) {
//
//    				if (log.isTraceEnabled())
//    					log.trace("duplicate: " + Arrays.toString(s.vals));
//
//    				// A duplicate solution exists.
//    				return false;
//    				
//    			}
//    			
//    		}
//    		
//			map.insert(s);
//    		
//    		return true;
//    		
//        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                        op.getChunkCapacity(), sink);

                state.filterSolutions(itr, stats, unsyncBuffer);

                unsyncBuffer.flush();
                
                sink.flush();
                
//                while (itr.hasNext()) {
//                    
//                    final IBindingSet[] a = itr.next();
//
//                    stats.chunksIn.increment();
//                    stats.unitsIn.add(a.length);
//
//                    // The distinct solutions accepted from this chunk. 
//                    final List<IBindingSet> accepted = new LinkedList<IBindingSet>();
//
//                    int naccepted = 0;
//
//                    for (IBindingSet bset : a) {
//
//						/*
//						 * Test to see if this solution is distinct from those
//						 * already seen.
//						 */
//                        final IConstant<?>[] vals = accept(bset);
//
//                        if (vals != null) {
//
//							/*
//							 * This is a distinct solution. Copy only the
//							 * variables used to select distinct solutions into
//							 * a new binding set and add that to the set of
//							 * [accepted] binding sets which will be emitted by
//							 * this operator.
//							 */
//                        	
//							final ListBindingSet tmp = new ListBindingSet();
//                        	
//							for (int i = 0; i < vars.length; i++) {
//
//                                if (vals[i] != null)
//                                    tmp.set(vars[i], vals[i]);
//
//							}
//							
//                            accepted.add(tmp);
//
//                            naccepted++;
//
//                        }
//
//                    }
//
//                    if (naccepted > 0) {
//
//						/*
//						 * At least one solution was accepted as distinct, so
//						 * copy the selected solutions to the output of the
//						 * operator.
//						 */
//                    	
//                        final IBindingSet[] b = accepted
//                                .toArray(new IBindingSet[naccepted]);
//                        
////                        System.err.println("output: "
////                                + Arrays.toString(b));
//
//                        // copy the distinct solutions to the output.
//                        sink.add(b);
//
////                        stats.unitsOut.add(naccepted);
////                        stats.chunksOut.increment();
//
//                    }
//
//                }
//
//                sink.flush();

                // done.
                return null;
                
            } finally {

                if(context.isLastInvocation()) {

                    state.release();
                    
                }
                
                sink.close();

            }

        }

//        /**
//         * Return the {@link HTree} object on which the distinct solutions are
//         * being written.
//         */
//        private HTree getMap(final DistinctBindingSetsWithHTreeOp op) {
//
//            /*
//             * First, see if the map already exists.
//             * 
//             * Note: Since the operator is not thread-safe, we do not need
//             * to use a putIfAbsent pattern here.
//             */
//            final IQueryAttributes attrs = context.getRunningQuery()
//                    .getAttributes();
//
//            final Object key = op.getId();
//            
//            HTree htree = (HTree) attrs.get(key);
//
//            if(htree != null) {
//
//                // Already exists.
//                return htree;
//                
//            }
//            
//            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
//
//            metadata.setAddressBits(op.getAddressBits());
//
//            metadata.setRawRecords(op.getRawRecords());
//
//            metadata.setMaxRecLen(op.getMaxRecLen());
//
//            metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.
//            
//            /*
//             * TODO This sets up a tuple serializer for a presumed case of 4
//             * byte keys (the buffer will be resized if necessary) and
//             * explicitly chooses the SimpleRabaCoder as a workaround since the
//             * keys IRaba for the HTree does not report true for isKeys(). Once
//             * we work through an optimized bucket page design we can revisit
//             * this as the FrontCodedRabaCoder should be a good choice, but it
//             * currently requires isKeys() to return true.
//             */
//            final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
//                    new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
//                    new SimpleRabaCoder(),// keys : TODO Optimize for int32!
//                    new SimpleRabaCoder() // vals
//            );
//
//            metadata.setTupleSerializer(tupleSer);
//
//            /*
//             * This wraps an efficient raw store interface around a child memory
//             * manager created from the IMemoryManager which is backing the
//             * query.
//             */
//            final IRawStore store = new MemStore(context.getRunningQuery()
//                    .getMemoryManager().createAllocationContext());
//
//            // Will support incremental eviction and persistence.
//            htree = HTree.create(store, metadata);
//
//            if (attrs.putIfAbsent(key, htree) != null) {
//
//                // This would indicate a concurrency problem.
//                throw new AssertionError();
//                
//            }
//            
//            return htree;
//
//        }
//        
//        /**
//         * Discard the map.
//         * <p>
//         * Note: The map can not be discarded (or cleared) until the last
//         * invocation.
//         */
//        private void release() {
//
//            final IRawStore store = map.getStore();
//            
//            map.close();
//            
//            store.close();
//
//        }
        
    } // class DistinctTask

}
