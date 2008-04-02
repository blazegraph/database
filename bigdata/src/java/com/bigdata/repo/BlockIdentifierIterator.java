package com.bigdata.repo;

import java.util.Iterator;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;

/**
 * Extracts the block identifier from the key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlockIdentifierIterator implements Iterator<Long> {
    
    final private String id;
    final private int version;
    final private ITupleIterator src;

    public String getId() {
        
        return id;
        
    }
    
    public int getVersion() {
        
        return version;
        
    }
    
    public BlockIdentifierIterator(String id, int version, ITupleIterator src) {
    
        if (id == null)
            throw new IllegalArgumentException();
        if (src == null)
            throw new IllegalArgumentException();
        
        this.id = id;
        this.version = version;
        this.src = src;
        
    }

    public boolean hasNext() {

        return src.hasNext();
        
    }

    public Long next() {

        ITuple tuple = src.next();
        
        byte[] key = tuple.getKey();
        
        long block = KeyBuilder.decodeLong(key, key.length
                - Bytes.SIZEOF_LONG);

        return block;
        
    }

    /**
     * Removes the last visited block for the file version.
     */
    public void remove() {

        src.remove();
        
    }
    
}