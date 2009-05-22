package com.bigdata.search;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
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
 */
public class ReadIndexTask implements Callable<Object> {

    final protected static Logger log = Logger.getLogger(ReadIndexTask.class);

//    /**
//     * True iff the {@link #log} level is INFO or less.
//     */
//    final protected static boolean INFO = log.isInfoEnabled();
//
//    /**
//     * True iff the {@link #log} level is DEBUG or less.
//     */
//    final protected static boolean DEBUG = log.isDebugEnabled();

    private final String queryTerm;
    private final boolean prefixMatch;
    private final double queryTermWeight;
//    private final FullTextIndex searchEngine;
    private final ConcurrentHashMap<Long, Hit> hits;
    private final ITupleIterator itr;

    /**
     * This instance is reused until it is consumed by a successful insertion
     * into {@link #hits} using
     * {@link ConcurrentHashMap#putIfAbsent(Object, Object)}. Once successfully
     * inserted, the {@link Hit#setDocId(long) docId} is set on the {@link Hit}
     * and a new instance is assigned to {@link #tmp}.
     */
    private Hit tmp = new Hit();

    /**
     * Setup a task that will perform a range scan for entries matching the
     * search term.
     * 
     * @param termText
     *            The term text for the search term.
     * @param prefixMatch
     *            When <code>true</code> any term having <i>termText</i> as a
     *            prefix will be matched. Otherwise the term must be an exact
     *            match for the <i>termText</i>.
     * @param queryTermWeight
     *            The weight for the search term.
     * @param searchEngine
     *            The search engine.
     * @param hits
     *            The map where the hits are being aggregated.
     */
    public ReadIndexTask(final String termText, final boolean prefixMatch,
            final double queryTermWeight, final FullTextIndex searchEngine,
            final ConcurrentHashMap<Long, Hit> hits) {

        if (termText == null)
            throw new IllegalArgumentException();
        
        if (searchEngine == null)
            throw new IllegalArgumentException();
        
        if (hits == null)
            throw new IllegalArgumentException();
        
        this.queryTerm = termText;

        this.prefixMatch = prefixMatch;
        
        this.queryTermWeight = queryTermWeight;

//        this.searchEngine = searchEngine;
        
        this.hits = hits;
     
        final IKeyBuilder keyBuilder = searchEngine.getKeyBuilder();
        
        final byte[] fromKey = FullTextIndex.getTokenKey(keyBuilder, termText,
                false/* successor */, 0L/* docId */, 0/* fieldId */);

        final byte[] toKey;
        
        // FIXME prefixMatch can not be turned off right now.
//        if (prefixMatch) {
            /*
             * Accepts anything starting with the search term. E.g., given
             * "bro", it will match "broom" and "brown" but not "break".
             */
            toKey = FullTextIndex.getTokenKey(keyBuilder, termText,
                    true/* successor */, Long.MIN_VALUE/* docId */, Integer.MIN_VALUE/* fieldId */);
//        } else {
//            /*
//             * Accepts only those entries that exactly match the search term.
//             */
//            toKey = FullTextIndex.getTokenKey(keyBuilder, termText+"\0",
//                    false/* successor */, 0L/* docId */, 0/* fieldId */);
//        }

        if (log.isDebugEnabled()) log.debug
//            System.err.println
            ("termText=[" + termText + "], prefixMatch=" + prefixMatch
                    + ", queryTermWeight=" + queryTermWeight + "\nfromKey="
                    + BytesUtil.toString(fromKey) + "\ntoKey="
                    + BytesUtil.toString(toKey));
        
        /*
         * @todo filter by field. all we need to do is pass in an array of the
         * fields that should be accepted and formulate and pass along an
         * ITupleFilter which accepts only those fields. Note that the docId is
         * in the last position of the key.
         */
        itr = searchEngine.getIndex()
                .rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/*filter*/);

    }
    
    /**
     * @return The #of fields with a hit on the search term as a
     *         {@link Long}.
     */
    public Long call() throws Exception {

        long nhits = 0;

        if (log.isDebugEnabled())
            log.debug("queryTerm=" + queryTerm + ", termWeight="
                    + queryTermWeight);

        final Thread t = Thread.currentThread();
        
        while (itr.hasNext()) {

            if (t.isInterrupted()) {

                if (log.isInfoEnabled())
                    log.info("Interrupted: queryTerm=" + queryTerm + ", nhits="
                            + nhits);
                
                break;
                
            }
            
            // next entry
            final ITuple tuple = itr.next();
            
            // key is {term,docId,fieldId}
//            final byte[] key = tuple.getKey();
//            
//            // decode the document identifier.
//            final long docId = KeyBuilder.decodeLong(key, key.length
//                    - Bytes.SIZEOF_LONG /*docId*/ - Bytes.SIZEOF_INT/*fieldId*/);

            final ByteArrayBuffer kbuf = tuple.getKeyBuffer();
            
            // decode the document identifier.
            final long docId = KeyBuilder.decodeLong(kbuf.array(), kbuf.limit()
                    - Bytes.SIZEOF_LONG /*docId*/ - Bytes.SIZEOF_INT/*fieldId*/);

            /*
             * Extract the term frequency and normalized term-frequency (term
             * weight) for this document.
             */
            final DataInputBuffer dis = tuple.getValueStream();
            final int termFreq = dis.readShort();
            final double termWeight = dis.readDouble();
            
            if (log.isDebugEnabled())
                log.debug("hit: term=" + queryTerm + ", docId=" + docId
                        + ", termFreq=" + termFreq + ", termWeight="
                        + termWeight + ", product="
                        + (queryTermWeight * termWeight));
            
            /*
             * Play a little magic to get the docId in the hit set without race
             * conditions.
             */
            final Hit hit;
            {
                Hit oldValue = hits.putIfAbsent(docId, tmp);
                if (oldValue == null) {
                    hit = tmp;
                    hit.setDocId(docId);
                    tmp = new Hit();
                } else {
                    hit = oldValue;
                }
            }
            
            hit.add( queryTerm, queryTermWeight * termWeight );
            
            nhits++;
            
        }

        return nhits;
        
    }
    
}