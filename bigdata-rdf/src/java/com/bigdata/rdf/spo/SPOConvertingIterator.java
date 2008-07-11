package com.bigdata.rdf.spo;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * Supports the bulk statement filter and bulk statement completion operations.
 * 
 * @version $Id: SPOConvertingIterator.java,v 1.2 2008/06/18 14:16:25
 *          thompsonbry Exp $
 */
public class SPOConvertingIterator implements IChunkedOrderedIterator<SPO> {
    
    private final static Logger log = Logger.getLogger(SPOConvertingIterator.class);
    
    private final IChunkedOrderedIterator<SPO> src;

    private final SPOConverter converter;

    private final IKeyOrder<SPO> keyOrder;

    private SPO[] converted = new SPO[0];

    private int pos = 0;

    public SPOConvertingIterator(IChunkedOrderedIterator<SPO> src, SPOConverter converter) {
        
        this(src, converter, src.getKeyOrder());
        
    }

    public SPOConvertingIterator(IChunkedOrderedIterator<SPO> src,
            SPOConverter converter, IKeyOrder<SPO> keyOrder) {
        
        this.src = src;
        
        this.converter = converter;
        
        this.keyOrder = keyOrder;
        
    }

    private SPO[] convert(SPO[] src) {

        return converter.convert(src);
        
    }

    public void close() {
        
        src.close();
        
    }

    public SPO next() {
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

    public IKeyOrder<SPO> getKeyOrder() {
        
        return keyOrder;
        
    }

    public SPO[] nextChunk() {
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
        SPO[] nextChunk = converted;
        converted = new SPO[0];
        pos = 0;
        return nextChunk;
    }

    public SPO[] nextChunk(IKeyOrder<SPO> keyOrder) {
        
        SPO[] chunk = nextChunk();
        
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
        
        SPO[] convert(SPO[] src);
        
    }

}
