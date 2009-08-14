package com.bigdata.btree.raba.codec;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.raba.AbstractRaba;
import com.bigdata.btree.raba.IRaba;

/**
 * An {@link IRabaDecoder} for use when the encoded logical byte[][] was
 * empty.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class EmptyRabaValueDecoder implements IRabaDecoder {

//    public static final EmptyRabaDecoder INSTANCE = new EmptyRabaDecoder();
    
    private final ByteBuffer data;
    
    public EmptyRabaValueDecoder(final ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException();
        
        this.data = data;
        
    }
    
    /**
     * Return an empty {@link ByteBuffer}.
     */
    final public ByteBuffer data() {
 
        return data;
        
    }

    final public boolean isReadOnly() {
        return true;
    }

    public boolean isKeys() {
        return false;
    }
    
    final public int capacity() {
        return 0;
    }

    final public int size() {
        return 0;
    }
    
    final public boolean isEmpty() {
        return true;
    }

    final public boolean isFull() {
        return true;
    }

    final public boolean isNull(int index) {
        throw new IndexOutOfBoundsException();
    }

    final public int length(int index) {
        throw new IndexOutOfBoundsException();
    }

    final public byte[] get(int index) {
        throw new IndexOutOfBoundsException();
    }

    final public int copy(int index, OutputStream os) {
        throw new IndexOutOfBoundsException();
    }

    final public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {

            public boolean hasNext() {
                return false;
            }

            public byte[] next() {
                throw new NoSuchElementException();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * If the {@link IRaba} represents B+Tree keys then returns <code>-1</code>
     * as the insertion point.
     * 
     * @throws UnsupportedOperationException
     *             unless the {@link IRaba} represents B+Tree keys.
     */
    final public int search(final byte[] searchKey) {
        
        if (isKeys())
            return -1;

        throw new UnsupportedOperationException();
        
    }

    /*
     * Mutation API is not supported.
     */
    
    final public int add(byte[] a) {
        throw new UnsupportedOperationException();
    }

    final public int add(byte[] value, int off, int len) {
        throw new UnsupportedOperationException();
    }

    final public int add(DataInput in, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    final public void set(int index, byte[] a) {
        throw new UnsupportedOperationException();
    }

    final public String toString() {

        return AbstractRaba.toString(this);

    }

}
