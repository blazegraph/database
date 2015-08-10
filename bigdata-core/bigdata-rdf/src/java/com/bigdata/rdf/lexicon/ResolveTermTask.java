package com.bigdata.rdf.lexicon;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBufferHandler;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Task resolves a chunk of {@link TermIV}s against the
 * {@link LexiconKeyOrder#ID2TERM} index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
class ResolveTermTask implements Callable<Void> {
    
    static private final transient Logger log = Logger
            .getLogger(ResolveTermTask.class);
    
    private final IIndex ndx;
    private final int fromIndex;
    private final int toIndex;
    private final byte[][] keys;
    private final TermId<?>[] notFound;
    private final ConcurrentHashMap<IV<?,?>, BigdataValue> map;
    private final ITermCache<IV<?,?>, BigdataValue> termCache;
    private final BigdataValueFactory valueFactory;

    /**
     * 
     * @param ndx
     *            The index that will be used to resolve the term
     *            identifiers.
     * @param fromIndex
     *            The first index in <i>keys</i> to resolve.
     * @param toIndex
     *            The first index in <i>keys</i> that will not be resolved.
     * @param keys
     *            The serialized term identifiers.
     * @param notFound
     *            An array of term identifiers whose corresponding
     *            {@link BigdataValue} must be resolved against the index.
     *            The indices in this array are correlated 1:1 with those
     *            for <i>keys</i>.
     * @param map
     *            Terms are inserted into this map under using their term
     *            identifier as the key. This is a concurrent map because
     *            the operation may have been split across multiple shards,
     *            in which case the updates to the map can be concurrent.
     */
    ResolveTermTask(final IIndex ndx, final int fromIndex,
            final int toIndex, final byte[][] keys, final TermId<?>[] notFound,
            final ConcurrentHashMap<IV<?,?>, BigdataValue> map,
            final ITermCache<IV<?,?>, BigdataValue> termCache,
            final BigdataValueFactory valueFactory) {

        this.ndx = ndx;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
        this.keys = keys;
        this.notFound = notFound;
        this.map = map;
        this.termCache = termCache;
        this.valueFactory = valueFactory;

    }

    public Void call() {
        
        // aggregates results if lookup split across index partitions.
        final ResultBufferHandler resultHandler = new ResultBufferHandler(
                toIndex, ndx.getIndexMetadata().getTupleSerializer()
                        .getLeafValuesCoder());

        // batch lookup
        ndx.submit(fromIndex, toIndex/* toIndex */, keys, null/* vals */,
                BatchLookupConstructor.INSTANCE, resultHandler);

        // the aggregated results.
        final ResultBuffer results = resultHandler.getResult();

        {

            final IRaba vals = results.getValues();
            
            for (int i = fromIndex; i < toIndex; i++) {

                final TermId<?> tid = notFound[i];

                final byte[] data = vals.get(i);

                if (data == null) {

                    log.warn("No such term: " + tid);

                    continue;

                }

                /*
                 * Note: This automatically sets the valueFactory reference
                 * on the de-serialized value.
                 */
                BigdataValue value = valueFactory.getValueSerializer()
                        .deserialize(data);
                
                // Set the term identifier.
                value.setIV(tid);

                final BigdataValue tmp = termCache.putIfAbsent(tid, value);

                if (tmp != null) {

                    value = tmp;

                }

                /*
                 * The term identifier was set when the value was
                 * de-serialized. However, this will throw an
                 * IllegalStateException if the value somehow was assigned
                 * the wrong term identifier (paranoia test).
                 */
                assert value.getIV().equals(tid) : "expecting tid=" + tid
                        + ", but found " + value.getIV();
                assert (value).getValueFactory() == valueFactory;

                // save in caller's concurrent map.
                map.put(tid, value);

            }

        }

        return null;
        
    }
    
}