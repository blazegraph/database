package com.bigdata.text;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;

/**
 * Procedure reads on the terms index, aggregating data on a per-{@link Hit}
 * basis.
 * <p>
 * The procedure uses an {@link IRangeQuery#rangeIterator(byte[], byte[])}
 * to perform a key range scan for a specific term. The range iterator will
 * automatically issue queries, obtaining a "chunk" of results at a time.
 * Those results are aggregated on the {@link Hit} collection, which is
 * maintained in a thread-safe hash map.
 * <p>
 * Note: An {@link ISplitHandler} imposes the constraint that index
 * partitions may only fall on a term boundary, hence all tuples for any
 * given term will always be found on the same index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME The {@link ISplitHandler}
 */
public class ReadIndexTask implements Callable<Object> {

    final public Logger log = Logger.getLogger(ReadIndexTask.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    private final String termText;
    private final double globalTermWeight;
    private final FullTextIndex searchEngine;
    private final ConcurrentHashMap<Long, Hit> hits;
    private final ITupleIterator itr;

    public ReadIndexTask(String termText, double globalTermWeight,
            FullTextIndex searchEngine, ConcurrentHashMap<Long, Hit> hits) {

        this.termText = termText;

        this.globalTermWeight = globalTermWeight;

        this.searchEngine = searchEngine;
        
        this.hits = hits;
     
        final IKeyBuilder keyBuilder = searchEngine.getKeyBuilder();
        
        final byte[] fromKey = searchEngine.getTokenKey(keyBuilder, termText,
                false/* successor */, 0L, 0);

        final byte[] toKey = searchEngine.getTokenKey(keyBuilder, termText,
                true/* successor */, 0L, 0);

        // @todo filter fields.
        // @todo values iff term weights were computed.
        itr = searchEngine.ndx
                .rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/*filter*/);
    }
    
    /**
     * @return The #of fields with a hit on the search term as a
     *         {@link Long}.
     */
    public Long call() throws Exception {
        
        long nhits = 0;
        
        while(itr.hasNext()) {

            if(Thread.currentThread().isInterrupted()) {
                
                log.info("Interrupted: term="+termText+", nhits="+nhits);
                
                break;
                
            }
            
            // next entry
            final ITuple tuple = itr.next();
            
            // key is {term,docId,fieldId}
            final byte[] key = tuple.getKey();
            
            // decode the document identifier.
            final long docId = KeyBuilder.decodeLong(key, key.length
                    - Bytes.SIZEOF_LONG /*docId*/ - Bytes.SIZEOF_INT/*fieldId*/);

            // FIXME extract the weight.
            double weight = 1d;
            
            if(DEBUG) {
                
                log.debug("hit: term="+termText+", docId="+docId+", weight="+weight);
                
            }
            
            /* FIXME can I reuse the [tmp] hit until it is consumed?  probably, but
             * then the docId needs to be set by the thread that successfully makes
             * the assignment.
             */
            Hit tmp = new Hit(docId);
            
            Hit oldValue = hits.putIfAbsent(docId, tmp);
            
            final Hit hit = (oldValue == null ? tmp : oldValue);
            
            assert hit != null;
            
            hit.add( weight );
            
            nhits++;
            
        }

        return nhits;
        
    }
    
}