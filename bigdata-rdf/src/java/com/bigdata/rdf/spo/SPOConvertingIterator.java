package com.bigdata.rdf.spo;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.rdf.util.KeyOrder;

/**
 * Supports the bulk filter.
 * 
 * @version $Id$
 */
public class SPOConvertingIterator implements ISPOIterator {
    
    private final static Logger log = Logger.getLogger(SPOConvertingIterator.class);
    
    private final ISPOIterator src;

    private final SPOConverter converter;

    private final KeyOrder keyOrder;

    private SPO[] converted = new SPO[0];

    private int pos = 0;

    public SPOConvertingIterator(ISPOIterator src, SPOConverter converter) {
        this(src, converter, src.getKeyOrder());
    }

    public SPOConvertingIterator(ISPOIterator src, SPOConverter converter,
            KeyOrder keyOrder) {
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

    public KeyOrder getKeyOrder() {
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

    public SPO[] nextChunk(KeyOrder keyOrder) {
        SPO[] chunk = nextChunk();
        Arrays.sort(chunk, keyOrder.getComparator());
        return chunk;
    }

    public static interface SPOConverter {
        SPO[] convert(SPO[] src);
    }
}
