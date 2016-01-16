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
 * Created on Aug 25, 2010
 */

package com.bigdata.bop.join;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.lexicon.ITermIVFilter;
import com.bigdata.rdf.spo.DistinctMultiTermAdvancer;
import com.bigdata.rdf.spo.DistinctTermAdvancer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * This operator performs a distinct terms scan for an {@link IPredicate},
 * binding the distinct values for the specified variable(s) from the
 * {@link IAccessPath} for the {@link IPredicate}. This is done using a
 * {@link DistinctTermAdvancer} to skip over any duplicate solutions in the
 * index. Thus the cost of this operator is O(N) where N is the number of
 * distinct solutions that exist in the index.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 * @see DistinctTermAdvancer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DistinctTermScanOp<E> extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	public interface Annotations extends AccessPathJoinAnnotations {

		/**
		 * The name of the variable whose distinct projection against the
		 * {@link IAccessPath} associated with the as-bound {@link IPredicate}
		 * is output by this operator.
		 */
		String DISTINCT_VAR = DistinctTermScanOp.class.getName()
				+ ".distinctVar";

    }

	/**
	 * Deep copy constructor.
	 * 
	 * @param op
	 */
	public DistinctTermScanOp(final DistinctTermScanOp<E> op) {
		
		super(op);
		
	}

	/**
	 * Shallow copy constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public DistinctTermScanOp(final BOp[] args,
			final Map<String, Object> annotations) {

		super(args, annotations);

		// MUST be given.
		getDistinctVar();
		getRequiredProperty(Annotations.PREDICATE);

		if (isOptional()) {
			
			/*
			 * TODO OPTIONAL is not implemented for this operator.
			 */
			
			throw new UnsupportedOperationException();

		}

	}

	public DistinctTermScanOp(final BOp[] args, final NV... annotations) {

		this(args, NV.asMap(annotations));

	}

	/**
	 * @see Annotations#DISTINCT_VAR
	 */
	protected IVariable<?> getDistinctVar() {
		
		return (IVariable<?>) getRequiredProperty(Annotations.DISTINCT_VAR);
		
	}
	
    /**
     * @see Annotations#SELECT
     */
    protected IVariable<?>[] getSelect() {

        return getProperty(Annotations.SELECT, null/* defaultValue */);

    }

    /**
     * @see Annotations#CONSTRAINTS
     */
    protected IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }

	@SuppressWarnings("unchecked")
	public IPredicate<E> getPredicate() {

		return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);

	}

    /**
     * Return the value of {@link IPredicate#isOptional()} for the
     * {@link IPredicate} associated with this join.
     * 
     * @see IPredicate.Annotations#OPTIONAL
     */
	private boolean isOptional() {

		return getPredicate().isOptional();

	}

	@Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<E>(this, context));

    }

    /**
     * Copy the source to the sink.
     */
    static private class ChunkTask<E> implements Callable<Void> {

        private final DistinctTermScanOp<E> op;

        private final BOpContext<IBindingSet> context;

        /**
         * The variable that gets bound to the distinct values by the scan.
         */
        private final IVariable<?> distinctVar;
        
		/**
		 * The source for the elements to be joined.
		 */
        private final IPredicate<E> predicate;
        
		/**
		 * The relation associated with the {@link #predicate} operand.
		 */
        private final IRelation<E> relation;
        
        ChunkTask(final DistinctTermScanOp<E> op,
                final BOpContext<IBindingSet> context) {

            this.op = op;

            this.context = context;

            this.distinctVar = op.getDistinctVar();
   
			this.predicate = op.getPredicate();
			
			this.relation = context.getRelation(predicate);

        }

        @Override
        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            // Convert source solutions to array (assumes low cardinality).
            final IBindingSet[] leftSolutions = BOpUtility.toArray(
                    context.getSource(), stats);

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

			final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
					op.getChunkCapacity(), sink);

            final IVariable<?>[] selectVars = op.getSelect();

            final IConstraint[] constraints = op.constraints();

    		try {

				/*
				 * TODO If there are multiple left solutions (from the pipeline)
				 * then we could generate their fromKeys and order them to
				 * improve cache locality. See PipelineJoin for an example of
				 * how this is done. For the distinct-term-scan this could
				 * provide a reasonable improvement in cache locality for the
				 * index.
				 */
				
				// For each source solution.
				for (IBindingSet bindingSet : leftSolutions) {

					// constrain the predicate to the given bindings.
					IPredicate<E> asBound = predicate.asBound(bindingSet);

                    if (asBound == null) {

                        /*
                         * This can happen for a SIDS mode join if some of the
                         * (s,p,o,[c]) and SID are bound on entry and they can not
                         * be unified. For example, the s position might be
                         * inconsistent with the Subject that can be decoded from
                         * the SID binding.
                         * 
                         * @see #815 (RDR query does too much work)
                         */

                        continue;

                    }

//                    if (partitionId != -1) {
//
//						/*
//						 * Constrain the predicate to the desired index
//						 * partition.
//						 * 
//						 * Note: we do this for scale-out joins since the
//						 * access path will be evaluated by a JoinTask
//						 * dedicated to this index partition, which is part
//						 * of how we give the JoinTask to gain access to the
//						 * local index object for an index partition.
//						 */
//
//						asBound = asBound.setPartitionId(partitionId);
//
//					}

        			/**
					 * The {@link IAccessPath} corresponding to the asBound
					 * {@link IPredicate} for this join dimension. The asBound
					 * {@link IPredicate} is {@link IAccessPath#getPredicate()}.
					 * 
					 * FIXME What do we do if there is a local filter or an
					 * access path filter? Do we have to NOT generate this
					 * operator? It is probably not safe to ignore those
					 * filters....
					 */
					final IAccessPath<E> accessPath = context.getAccessPath(
							relation, asBound);

					if (accessPath.getPredicate().getIndexLocalFilter() != null) {
						// index has local filter. requires scan.
						throw new AssertionError();
					}

					if (accessPath.getPredicate().getAccessPathFilter() != null) {
						// access path filter exists. requires scan.
						throw new AssertionError();
					}

					// TODO Cast to AccessPath is not type safe.
					final IChunkedIterator<IV> rightItr = distinctTermScan(
							(AccessPath<E>) accessPath, null/* termIdFilter */);

					while (rightItr.hasNext()) {
						
						// New binding set.
						final IBindingSet right = new ListBindingSet();

						// Bind the distinctTermVar.
						right.set(distinctVar, new Constant(rightItr.next()));
						
						// See if the solutions join.
						final IBindingSet outSolution = BOpContext.bind(//
								bindingSet,// left
								right,//
								constraints,//
								selectVars//
								);

						if (outSolution != null) {

							// Output the solution.
							unsyncBuffer.add(outSolution);

						}

					}
					
				}
                
	            // flush the unsync buffer.
	            unsyncBuffer.flush();
                
                // flush the sink.
                sink.flush();

                // Done.
                return null;
                
            } finally {

                sink.close();
                
                context.getSource().close();
                
            }

        }

        /**
		 * Efficient scan of the distinct term identifiers that appear in the
		 * first position of the keys for the statement index corresponding to
		 * the specified {@link IKeyOrder}. For example, using
		 * {@link SPOKeyOrder#POS} will give you the term identifiers for the
		 * distinct predicates actually in use within statements in the
		 * {@link SPORelation}.
		 * 
		 * @param keyOrder
		 *            The selected index order.
		 * @param fromKey
		 *            The first key for the scan -or- <code>null</code> to start
		 *            the scan at the head of the index.
		 * @param toKey
		 *            The last key (exclusive upper bound) for the scan -or-
		 *            <code>null</code> to scan until the end of the index.
		 * @param termIdFilter
		 *            An optional filter on the visited {@link IV}s.
		 * 
		 * @return An iterator visiting the distinct term identifiers.
		 * 
		 *         TODO Move this method to {@link AccessPath}. Also, refactor
		 *         {@link SPORelation#distinctTermScan(IKeyOrder)} to use this
		 *         code.
		 */
		private static <E> IChunkedIterator<IV> distinctTermScan(
				final AccessPath<E> ap, final ITermIVFilter termIdFilter) {

        	final IKeyOrder<E> keyOrder = ap.getKeyOrder();
        	
        	final byte[] fromKey = ap.getFromKey();
        	
        	final byte[] toKey = ap.getToKey();
        	
        	// if there are predicate positions bound to constants, we use
        	// the distinct multi term advancer, otherwise the simple distinct
        	// term advancer is sufficient
        	List<BOp> predicateArgs = ap.getPredicate().args();
        	int nrConsts = 0;
        	for (int i=0; i<predicateArgs.size(); i++) {
        		if (predicateArgs.get(i) instanceof IConstant) {
        			nrConsts++;
        		}
        	}
        	final int nrConstsFinal = nrConsts;
        	
			final Advancer<SPO> filter = nrConsts==0 ?
				new DistinctTermAdvancer(keyOrder.getKeyArity()) :
				new DistinctMultiTermAdvancer(keyOrder.getKeyArity(), nrConsts);
            
            /*
             * Layer in the logic to advance to the tuple that will have the
             * next distinct term identifier in the first position of the key.
             */

            if (termIdFilter != null) {

                /*
                 * Layer in a filter for only the desired term types.
                 */
                
                filter.addFilter(new TupleFilter<E>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean isValid(final ITuple<E> tuple) {

                        final byte[] key = tuple.getKey();
                        final IV[] ivs = IVUtility.decode(key,nrConstsFinal+1);
                        final IV iv = ivs[nrConstsFinal];
                        return termIdFilter.isValid(iv);
                    }

                });

            }

            @SuppressWarnings("unchecked")
            final Iterator<IV> itr = new Striterator(ap.getIndex(/*keyOrder*/)
                    .rangeIterator(fromKey, toKey,//
                            0/* capacity */, IRangeQuery.KEYS | IRangeQuery.CURSOR,
                            filter)).addFilter(new Resolver() {
                        
                        private static final long serialVersionUID = 1L;
                        
                        /**
                         * Resolve tuple to IV.
                         */
                        @Override
						protected IV resolve(final Object obj) {

							final byte[] key = ((ITuple<?>) obj).getKey();
                            
							final IV[] ivs = IVUtility.decode(key,nrConstsFinal+1);
							return ivs[nrConstsFinal];
                        }
                        
                    });

			return new ChunkedWrappedIterator<IV>(itr, ap.getChunkCapacity(),
					IV.class);
                    
        }
        
    } // class ChunkTask

    
}
