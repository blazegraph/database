package com.bigdata.bop.ap;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.bop.AbstractBOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpList;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.relation.rule.BindingSetSortKeyBuilder;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.DistinctFilter;
import com.bigdata.striterator.IChunkConverter;
import com.bigdata.striterator.MergeFilter;

/**
 * A DISTINCT operator based on a hash table.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 * 
 * @todo could have an implementation backed by a persistent hash map using an
 *       extensible hash function to automatically grow the persistence store.
 *       This could be a general purpose persistent hash functionality, but it
 *       could also operate against a temporary file when used in the context of
 *       a query (the backing file can be destroyed afterwards or the data can
 *       be simply written onto the current temporary store).
 * 
 * @todo Consider the use of lock amortization (batching) to reduce contention
 *       for the backing map. Alternatively, we could accept entire blocks of
 *       elements from a single source at a time, which would single thread us
 *       through the map. Or bound the #of threads hitting the map at once,
 *       increase the map concurrency level, etc.
 * 
 * @todo Reconcile with {@link IChunkConverter}, {@link DistinctFilter} (handles
 *       solutions) and {@link MergeFilter} (handles comparables).
 */
public class DistinctElementFilter<E> 
extends AbstractBOp
//extends AbstractChunkedIteratorOp<E>
//implements IElementFilter<E>,
//        implements IConstraint, 
//        implements ChunkedIteratorOp<E>
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations {

        String INITIAL_CAPACITY = "initialCapacity";

        String LOAD_FACTOR = "loadFactor";

        String CONCURRENCY_LEVEL = "concurrencyLevel";
        
    }

    public DistinctElementFilter(final IVariable<?>[] distinctList, final UUID masterUUID) {

        super(distinctList, NV.asMap(new NV[] { new NV(Annotations.QUERY_ID,
                masterUUID),
        // new NV(Annotations.BOP_ID, bopId)
                }));

        if (masterUUID == null)
            throw new IllegalArgumentException();

    }

//    public Future<Void> eval(final IBigdataFederation<?> fed,
//            final IJoinNexus joinNexus, final IBlockingBuffer<E[]> buffer) {
//
//        final FutureTask<Void> ft = new FutureTask<Void>(new DHTTask(joinNexus,
//                buffer));
//
//        joinNexus.getIndexManager().getExecutorService().execute(ft);
//
//        return ft;
//
//    }

    /**
     * Task executing on the node.
     */
    private class DHTTask implements Callable<Void> {

        private final IJoinNexus joinNexus;

        private final IBlockingBuffer<E[]> buffer;

        private final ConcurrentHashMap<byte[], Void> map;

        /* Note: This is NOT thread safe! */
        private final BindingSetSortKeyBuilder sortKeyBuilder;
        
        DHTTask(final IJoinNexus joinNexus,
                final IBlockingBuffer<E[]> buffer) {

            this.joinNexus = joinNexus;
            
            this.buffer = buffer;

            final IVariable<?>[] vars = ((BOpList) args[0/* distinctList */])
                    .toArray(new IVariable[0]);

            this.sortKeyBuilder = new BindingSetSortKeyBuilder(KeyBuilder
                    .newInstance(), vars);

            this.map = new ConcurrentHashMap<byte[], Void>(/*
                     * @todo initialCapacity using annotations
                     * @todo loadFactor ...
                     * @todo concurrencyLevel ...
                     */);
        }

        private boolean accept(final IBindingSet bset) {

            return map.putIfAbsent(sortKeyBuilder.getSortKey(bset), null) == null;

        }

        public Void call() throws Exception {

            /*
             * FIXME Setup to drain binding sets from the source. Note that the
             * sort key builder is not thread safe, so a pool of key builders
             * with a non-default initial capacity (LT 1024) might be used to
             * allow higher concurrency for key building.
             * 
             * Alternatively, the caller could generate the keys (SOUNDS GOOD)
             * and just ship the byte[] keys to the DHTFilter.
             * 
             * The DHTFilter needs to send back its boolean[] responses bit
             * coded or run length coded. See AbstractArrayIndexProcedure which
             * already does some of that. Those responses should move through
             * NIO Buffers just like everything else, but the response will be
             * much smaller than the incoming byte[][] (aka IRaba).
             */
            throw new UnsupportedOperationException();

        }

    }

    // public ResultBitBuffer bulkFilter(final K[] elements) {
    //            
    // }

}
