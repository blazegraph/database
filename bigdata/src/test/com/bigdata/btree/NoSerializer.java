package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;

import com.bigdata.io.DataOutputBuffer;

/**
 * A (De-)serializer that always throws exceptions.  This is used when we
 * are testing in a context in which incremental IOs are disabled, e.g.,
 * by the {@link NoEvictionListener}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NoSerializer implements IValueSerializer {

    /**
     * 
     */
    private static final long serialVersionUID = -6467578720380911380L;
    
    public transient static final NoSerializer INSTANCE = new NoSerializer();
    
    public NoSerializer() {}
    
    public void getValues(DataInput is, byte[][] values, int n) throws IOException {

        throw new UnsupportedOperationException();

    }

    public void putValues(DataOutputBuffer os, byte[][] values, int n) throws IOException {

        throw new UnsupportedOperationException();

    }

}