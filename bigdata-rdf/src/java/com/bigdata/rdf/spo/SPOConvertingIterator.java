package com.bigdata.rdf.spo;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Supports the bulk statement filter and bulk statement completion operations.
 * 
 * @version $Id: SPOConvertingIterator.java,v 1.2 2008/06/18 14:16:25
 *          thompsonbry Exp $
 */
public class SPOConvertingIterator implements IChunkedOrderedIterator<ISPO> {
    
    private final static Logger log = Logger.getLogger(SPOConvertingIterator.class);
    
    private final IChunkedOrderedIterator<ISPO> src;

    private final SPOConverter converter;

    private final IKeyOrder<ISPO> keyOrder;

    private ISPO[] converted = new ISPO[0];

    private int pos = 0;

    public SPOConvertingIterator(IChunkedOrderedIterator<ISPO> src, SPOConverter converter) {
        
        this(src, converter, src.getKeyOrder());
        
    }

    public SPOConvertingIterator(IChunkedOrderedIterator<ISPO> src,
            SPOConverter converter, IKeyOrder<ISPO> keyOrder) {
        
        this.src = src;
        
        this.converter = converter;
        
        this.keyOrder = keyOrder;
        
    }

    private ISPO[] convert(ISPO[] src) {

        return converter.convert(src);
        
    }

    public void close() {
        
        src.close();
        
    }

    public ISPO next() {
        if (pos >= converted.length && src.hasNext()) {
            // convert the next chunk
            converted = convert(src.nextChunk());
            pos = 0;
        }
        if(log.isInfoEnabled())
        log.info("returning converted["+pos+"]");
        return converted[pos++];
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean hasNext() {
        if (pos >= converted.length && src.hasNext()) {
            // convert the next chunk
            converted = convert(src.nextChunk());
            pos = 0;
        }
        boolean hasNext = pos < converted.length;
        if(log.isInfoEnabled()) log.info(hasNext);
        // StringWriter sw = new StringWriter();
        // new Exception("stack trace").printStackTrace(new PrintWriter(sw));
        // log.info(sw.toString());
        return hasNext;
    }

    public IKeyOrder<ISPO> getKeyOrder() {
        
        return keyOrder;
        
    }

    public ISPO[] nextChunk() {
        if (pos >= converted.length && src.hasNext()) {
            // convert the next chunk
            converted = convert(src.nextChunk());
            pos = 0;
        }
        if (pos > 0) {
            SPO[] chunk = new SPO[converted.length - pos];
            System.arraycopy(converted, pos, chunk, 0, chunk.length);
            converted = chunk;
            pos = 0;
        }
        ISPO[] nextChunk = converted;
        converted = new ISPO[0];
        pos = 0;
        return nextChunk;
    }

    public ISPO[] nextChunk(IKeyOrder<ISPO> keyOrder) {
        
        ISPO[] chunk = nextChunk();
        
        Arrays.sort(chunk, keyOrder.getComparator());
        
        return chunk;
        
    }

    /**
     * This is a chunk at a time type processor (SPO to SPO). Elements can be
     * dropped, have their state changed, or have their state replaced by a
     * "completed" element (e.g., one with the {@link StatementEnum} and
     * statement identifier).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface SPOConverter {
        
        ISPO[] convert(ISPO[] src);
        
    }

}
