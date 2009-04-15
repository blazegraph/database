package com.bigdata.search;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Token;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KV;
import com.bigdata.btree.proc.IntegerAggregator;
import com.bigdata.io.ByteArrayBuffer;

/**
 * A buffer holding tokens extracted from one or more documents / fields.
 * Each entry in the buffer corresponds to the {@link TermFrequencyData}
 * extracted from a field of some document. When the buffer overflows it is
 * {@link #flush()}, writing on the indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TokenBuffer {

    final protected static Logger log = Logger.getLogger(TokenBuffer.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static  boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static  boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /** The object on which the buffer will write when it overflows. */
    private FullTextIndex textIndexer;
    
    /** The capacity of the {@link #buffer}. */
    private final int capacity;
    
    /** Each entry models a field of some document. */
    private final TermFrequencyData[] buffer;
    
    /** #of entries in the {@link #buffer}. */
    private int count = 0;
    
    /** #of distinct {docId} values in the {@link #buffer}. */
    private int ndocs;
    
    /** #of distinct {docId,fieldId} values in the {@link #buffer}. */
    private int nfields;
    
    /** #of distinct {docId,fieldId,termText} tuples.*/
    private int nterms;
    
    /** The last observed docId and -1L if none observed. */
    private long lastDocId;

    /** The last observed fieldId and -1 if none observed. */
    private long lastFieldId;

    /**
     * Ctor.
     * @param capacity
     *            The #of distinct {document,field} tuples that can be held in
     *            the buffer before it will overflow. The buffer will NOT
     *            overflow until you exceed this capacity.
     * 
     * @param textIndexer
     *            The object on which the buffer will write when it overflows or
     *            is {@link #flush()}ed.
     */
    public TokenBuffer(int capacity, FullTextIndex textIndexer) {
        
        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (textIndexer == null)
            throw new IllegalArgumentException();
        
        this.capacity = capacity;
        
        this.textIndexer = textIndexer;
        
        buffer = new TermFrequencyData[capacity];
        
        reset();
        
    }
    
    /**
     * Discards all data in the buffer and resets it to a clean state.
     */
    public void reset() {
        
        for (int i = 0; i < count; i++) {

            buffer[i] = null;

        }
        
        count = 0;
        
        ndocs = 0;
        
        nfields = 0;
        
        nterms = 0;

        lastDocId = -1L;
        
        lastFieldId = -1;
        
    }
    
    /**
     * The #of entries in the buffer.
     */
    public int size() {
        
        return count;
        
    }

    /**
     * Return the {@link TermFrequencyData} for the specified index.
     * 
     * @param index
     *            The index in [0:<i>count</i>).
     *            
     * @return The {@link TermFrequencyData} at that index.
     * 
     * @throws IndexOutOfBoundsException
     */
    public TermFrequencyData get(final int index) {
        
        if (index < 0 || index >= count)
            throw new IndexOutOfBoundsException(); 
        
        return buffer[index];
        
    }
    
    /**
     * Adds another token to the current field of the current document. If
     * either the field or the document identifier changes, then begins a
     * new field and possibly a new document. If the buffer is full then it
     * will be {@link #flush()}ed before beginning a new field.
     * <p>
     * Note: This method is NOT thread-safe.
     * <p>
     * Note: There is an assumption that the caller will process all tokens
     * for a given field in the same document at once. Failure to do this
     * will lead to only part of the term-frequency distribution for the
     * field being captured by the indices.
     * 
     * @param docId
     *            The document identifier.
     * @param fieldId
     *            The field identifier.
     * @param token
     *            The token.
     */
    public void add(final long docId, final int fieldId, final Token token) {

        if (DEBUG) {

            log.debug("docId=" + docId + ", fieldId=" + fieldId + ", token="
                    + token);
            
        }
        
        final boolean newField;
        
        if (count == 0) {

            // first tuple in clean buffer.
            
            ndocs++;

            nfields++;
            
            lastDocId = docId;
            
            lastFieldId = fieldId;
            
            newField = true;
            
        } else {
            
            if (lastDocId != docId) {

                // start of new document
                ndocs++;

                // also start of new field.
                nfields++;

                newField = true;

                // normalize the last term-frequency distribution.
                buffer[count-1].normalize();

            } else if (lastFieldId != fieldId) {

                // start of new field in same document.
                nfields++;

                newField = true;

                // normalize the last term-frequency distribution.
                buffer[count-1].normalize();

            } else {
                
                newField = false;

            }
            
        }
        
        if (newField && count == capacity) {
            
            flush();
            
        }

        if(newField) {

            buffer[count++] = new TermFrequencyData(docId, fieldId, token);
            
            nterms++;
            
        } else {
            
            if( buffer[count-1].add(token) ) {
                
                nterms++;
                
            }
            
        }

        lastDocId = docId;

        lastFieldId = fieldId;
        
    }

    /**
     * Write any buffered data on the indices.
     * <p>
     * Note: The writes on the terms index are scattered since the key for the
     * index is {term, docId, fieldId}. This method will batch up and then apply
     * a set of updates, but the total operation is not atomic. Therefore search
     * results which are concurrent with indexing may not have access to the
     * full data for concurrently indexed documents. This issue may be resolved
     * by allowing the indexer to write ahead and using a historical commit time
     * for the search.
     * <p>
     * Note: If a document is pre-existing, then the existing data for that
     * document MUST be removed unless you know that the fields to be found in
     * the will not have changed (they may have different contents, but the same
     * fields exist in the old and new versions of the document).
     */
    public void flush() {

        if (nterms == 0) {
            
            reset();
            
            return;
            
        }
        
        if (INFO)
            log.info("count=" + count + ", ndocs=" + ndocs + ", nfields="
                    + nfields + ", nterms=" + nterms);

        /*
         * Generate keys[] and vals[].
         */

        // array of correlated key/value tuples.
        final KV[] a = new KV[nterms];

        // Note: reused for each {doc,field,token} tuple key.
        final IKeyBuilder keyBuilder = textIndexer.getKeyBuilder();

        // Note: reused for each {doc,field,token} tuple value.
        final ByteArrayBuffer buf = new ByteArrayBuffer();

        // #of {term,doc,field} tuples generated
        int n = 0;

        // for each {doc,field} tuple.
        for (int i = 0; i < count; i++) {

            final TermFrequencyData termFreq = buffer[i];
            
            final long docId = termFreq.docId;

            final int fieldId = termFreq.fieldId;
            
            // process in an arbitrary order.
            final Iterator<TermMetadata> itr = termFreq.terms.values().iterator();

            // emit {term,doc,field} tuples.
            while(itr.hasNext()) {
            
                final TermMetadata termMetadata = itr.next();

                final String termText = termMetadata.termText();
                
                final byte[] key = FullTextIndex.getTokenKey(keyBuilder, termText,
                        false/* successor */, docId, fieldId);
                
                if(DEBUG) {
                    log.debug("{" + termText + "," + docId + "," + fieldId
                            + "}: #occurences="
                            + termMetadata.occurrences.size());
                }
                
                final byte[] val = textIndexer.getTokenValue(buf, termMetadata);

                // save in the correlated array.
                a[n++] = new KV(key, val);

            }

        }
        
        assert n == nterms : "ntokens=" + nterms + ", n=" + n;
        
        // Sort {term,docId,fieldId}:{value} tuples into total index order. 
        Arrays.sort(a);

        /*
         * Copy the correlated key:val data into keys[] and vals[] arrays.
         * 
         * @todo vals[] not used if only the token presence is recorded.
         */

        final byte[][] keys = new byte[nterms][];
        
        final byte[][] vals = new byte[nterms][];
        
        for (int i = 0; i < nterms; i++) {
            
            keys[i] = a[i].key;

            vals[i] = a[i].val;
            
        }

        // Batch write on the index.
        writeOnIndex(n, keys, vals);

        // Clear the buffer.
        reset();
        
    }
    
    /**
     * Writes on the index.
     *  
     * @param n
     * @param keys
     * @param vals
     * 
     * @return The #of pre-existing records that were updated.
     */
    protected long writeOnIndex(final int n, final byte[][] keys,
            final byte[][] vals) {

        final IntegerAggregator resultHandler = new IntegerAggregator();
        
        textIndexer.getIndex().submit(0, //fromIndex
                n, // toIndex
                keys,//
                vals,//
                (textIndexer.isOverwrite() //
                        ? TextIndexWriteProc.IndexWriteProcConstructor.OVERWRITE
                        : TextIndexWriteProc.IndexWriteProcConstructor.NO_OVERWRITE//
                        ),//
                 resultHandler//
                );
        
        return resultHandler.getResult();
        
    }
    
}
