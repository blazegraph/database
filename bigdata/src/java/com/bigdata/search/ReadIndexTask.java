package com.bigdata.search;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISimpleSplitHandler;
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
 * Note: An {@link ISimpleSplitHandler} imposes the constraint that index
 * partitions may only fall on a term boundary, hence all tuples for any
 * given term will always be found on the same index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadIndexTask implements Callable<Object> {

    final private static Logger log = Logger.getLogger(ReadIndexTask.class);

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
//    private final boolean prefixMatch;
//    private final int exactMatchLength;
    private final double queryTermWeight;
    private final boolean fieldsEnabled;
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

//        this.prefixMatch = prefixMatch;
        
        this.queryTermWeight = queryTermWeight;

        this.fieldsEnabled = searchEngine.isFieldsEnabled();
        
//        this.searchEngine = searchEngine;
        
        this.hits = hits;
     
        final IKeyBuilder keyBuilder = searchEngine.getKeyBuilder();

//        if (!prefixMatch) {
//            /*
//             * Figure out how many bytes are in the Unicode sort key for the
//             * termText. In order to be an exact match, the visited tuples may
//             * not have more than this many bytes before the start of the docId
//             * field. (It is not possible for them to have fewer bytes since the
//             * Unicode sort key prefix length will be the same for both the
//             * fromKey and the toKey. The Unicode sort key for the toKey is
//             * formed by adding one to the LSB position).
//             */
//            
//            keyBuilder
//                    .appendText(termText, true/* unicode */, false/* successor */);
//            
//            exactMatchLength = keyBuilder.getLength();
//            
//        } else {
//            
//            // ignored.
//            exactMatchLength = -1;
//            
//        }
        
        /*
         * FIXME This would appear to start in the middle of the docId and
         * fieldId value space since I would assume that Long.MIN_VALUE is the
         * first docId.
         */
        final byte[] fromKey = FullTextIndex.getTokenKey(keyBuilder, termText,
                false/* successor */, fieldsEnabled, Long.MIN_VALUE/* docId */,
                Integer.MIN_VALUE/* fieldId */);

        final byte[] toKey;
        
        // FIXME prefixMatch can not be turned off right now.
        if (prefixMatch) {
            /*
             * Accepts anything starting with the search term. E.g., given
             * "bro", it will match "broom" and "brown" but not "break".
             */
        toKey = FullTextIndex.getTokenKey(keyBuilder, termText,
                true/* successor */, fieldsEnabled, Long.MIN_VALUE/* docId */,
                Integer.MIN_VALUE/* fieldId */);
        } else {
            /*
             * Accepts only those entries that exactly match the search term.
             */
            toKey = FullTextIndex.getTokenKey(keyBuilder, termText,
                    false/* successor */, fieldsEnabled,
                    Long.MAX_VALUE/* docId */, Integer.MAX_VALUE/* fieldId */);
        }

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

            // don't test for interrupted on each result -- too much work.
            if (nhits % 100 == 0 && t.isInterrupted()) {

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

            /*
             * The byte offset of the docId in the key.
             * 
             * Note: This is also the byte length of the match on the unicode
             * sort key, which appears at the head of the key.
             */
            final int docIdOffset = kbuf.limit() - Bytes.SIZEOF_LONG /* docId */
                    - (fieldsEnabled ? Bytes.SIZEOF_INT/* fieldId */: 0);

//            if (!prefixMatch && docIdOffset != exactMatchLength) {
//             
//                /*
//                 * The Unicode sort key associated with this tuple is longer
//                 * than the given token - hence it can not be an exact match.
//                 */
//                
//                continue;
//                
//            }
            
            // decode the document identifier.
            final long docId = KeyBuilder.decodeLong(kbuf.array(), docIdOffset);

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