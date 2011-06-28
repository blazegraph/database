package com.bigdata.relation.rule.eval.pipeline;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * Implementation used to write on the {@link JoinTask#getSolutionBuffer()}
 * for the last join dimension. The solution buffer is either an
 * {@link IBlockingBuffer} (for query) or a buffer that writes on the head
 * relation for the rule (for mutation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
class UnsynchronizedSolutionBuffer<E extends IBindingSet> extends
        UnsynchronizedOutputBuffer<E> {

    private final IJoinNexus joinNexus;
    /**
     * An optional filter on the generated {@link ISolution}s. This filter
     * is obtained from {@link IJoinNexus#getSolutionFilter()}. When non-<code>null</code>,
     * {@link #handleChunk(IBindingSet[])} applies the filter to keep
     * {@link ISolution}s licensed by the {@link IBindingSet}s that do not
     * meet constraint imposed by that filter. (For RDF, this is used to
     * keep literals out of the subject position, keep axioms from being
     * written into the DB by inference, etc.)
     */
    private final IElementFilter<ISolution> solutionFilter;
    
    public UnsynchronizedSolutionBuffer(final JoinTask joinTask,
            final IJoinNexus joinNexus, final int capacity) {

        super(joinTask, capacity);

        this.joinNexus = joinNexus;
        
        // Note: MAY be null.
        this.solutionFilter = joinNexus.getSolutionFilter();
        
    }
    
    /**
     * Generate a chunk of {@link ISolution}s for the accepted
     * {@link IBindingSet}s and add those those {@link ISolution}s to the
     * {@link JoinTask#getSolutionBuffer()}. For query, that will be a
     * (proxy for) the {@link IJoinNexus#newQueryBuffer()} created by the
     * {@link JoinMasterTask}. For mutation, that will be a buffer created
     * for the {@link JoinTask} instance (this avoids have all data for
     * mutation flow through the master).
     * 
     * @throws BufferClosedException
     *             If the {@link IBuffer} returned by
     *             {@link JoinTask#getSolutionBuffer()} is an
     *             {@link IBlockingBuffer} which has been closed. This will
     *             occur for query if the query specifies a SLICE and the
     *             SLICE has been satisified. Under these conditions the
     *             {@link IBlockingBuffer} will be closed asynchronously by
     *             the query consumer and {@link BufferClosedException} will
     *             be thown by {@link IBlockingBuffer#add(Object)}.
     */
    protected void handleChunk(final E[] chunk) {

        final IBuffer<ISolution[]> solutionBuffer = joinTask
                .getSolutionBuffer();

        final IRule rule = joinTask.rule;
        
        ISolution[] a = new ISolution[chunk.length];

        int naccepted = 0;

        for (int i = 0; i < chunk.length; i++) {

            // an accepted binding set.
            final IBindingSet bindingSet = chunk[i];

            /*
             * Note: The [joinNexus] MUST have access to the global and
             * mutable index views. For the federation, this means that it
             * is not the same instance that you are using to read on the
             * access path!
             */
            final ISolution solution = joinNexus.newSolution(rule,
                    bindingSet);

            if (solutionFilter == null || solutionFilter.isValid(solution)) {

                a[naccepted++] = solution;

            }

        }

        if (naccepted == 0)
            return;
        
        if (naccepted < chunk.length) {

            // Make the array dense and snug.
            
            final ISolution[] b = new ISolution[naccepted];
            
            System.arraycopy(a, 0, b, 0, naccepted);
            
            a = b;
            
        }
        
        /*
         * Add the chunk to the [solutionBuffer].
         * 
         * Note: This can throw a BufferClosedException. In particular, this
         * exception will be thrown if the [solutionBuffer] is a query
         * buffer and a SLICE been satisified causing the [solutionBuffer]
         * to be asynchronously closed by the query consumer.
         */

        solutionBuffer.add(a);

        joinTask.stats.bindingSetChunksOut++;
        joinTask.stats.bindingSetsOut += naccepted;

    }

}