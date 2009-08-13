package com.bigdata.btree.raba.codec;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@link IRabaDecoder} for use when the encoded logical byte[][] was
 * empty.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class EmptyRabaDecoder implements IRabaDecoder {

    public static final EmptyRabaDecoder INSTANCE = new EmptyRabaDecoder();
    
    private final ByteBuffer data = ByteBuffer.allocate(0);
    
    private EmptyRabaDecoder() {
    }
    
    /**
     * Return an empty {@link ByteBuffer}.
     */
    public ByteBuffer data() {
        return data;
    }

    public boolean isReadOnly() {
        return true;
    }

    public boolean isNullAllowed() {
        return true;
    }
    
    public boolean isSearchable() {
        return true;
    }
    
    public int capacity() {
        return 0;
    }

    public int size() {
        return 0;
    }
    
    public boolean isEmpty() {
        return true;
    }

    public boolean isFull() {
        return true;
    }

    public boolean isNull(int index) {
        throw new IndexOutOfBoundsException();
    }

    public int length(int index) {
        throw new IndexOutOfBoundsException();
        }

    public byte[] get(int index) {
        throw new IndexOutOfBoundsException();
    }

    public int copy(int index, OutputStream os) {
        throw new IndexOutOfBoundsException();
    }

    public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>(){

            public boolean hasNext() {
                return false;
            }

            public byte[] next() {
                throw new NoSuchElementException();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }};
    }

    /**
     * Always returns <code>-1</code> as the insertion point.
     */
    public int search(final byte[] searchKey) {
        
        return -1;
        
    }

    /*
     * Mutation API is not supported.
     */
    
    public int add(byte[] a) {
        throw new UnsupportedOperationException();
    }

    public int add(byte[] value, int off, int len) {
        throw new UnsupportedOperationException();
    }

    public int add(DataInput in, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void set(int index, byte[] a) {
        throw new UnsupportedOperationException();
    }

}
