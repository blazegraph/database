package com.bigdata.btree.raba;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Iterator;


import cutthecrap.utils.striterators.EmptyIterator;

/**
 * An immutable, empty {@link IRaba}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public abstract class EmptyRaba implements IRaba, Externalizable {

    /**
     * An empty, immutable B+Tree keys {@link IRaba} instance.
     */
    public static transient IRaba KEYS = new EmptyKeysRaba();

    /**
     * An empty, immutable B+Tree values {@link IRaba} instance.
     */
    public static transient IRaba VALUES = new EmptyValuesRaba();

    /**
     * An empty, immutable B+Tree keys {@link IRaba}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class EmptyKeysRaba extends EmptyRaba {
        
        /**
         * De-serialization ctor.
         */
        public EmptyKeysRaba() {
            
        }
        
        final public boolean isKeys() {

            return true;
            
        }

    }
    
    /**
     * An empty, immutable B+Tree values {@link IRaba}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class EmptyValuesRaba extends EmptyRaba {
        
        /**
         * De-serialization ctor.
         */
        public EmptyValuesRaba() {
            
        }

        final public boolean isKeys() {

            return false;
            
        }

    }
    
    /**
     * De-serialization ctor.
     */
    public EmptyRaba() {
        
    }
    
    final public int capacity() {
        return 0;
    }

    final public boolean isEmpty() {
        return true;
    }

    final public boolean isFull() {
        return true;
    }

    final public int size() {
        return 0;
    }
    
    final public boolean isReadOnly() {
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

    @SuppressWarnings("unchecked")
    final public Iterator<byte[]> iterator() {
        return EmptyIterator.DEFAULT;
    }

    final public int search(byte[] searchKey) {
        if (isKeys())
            return -1;
        throw new UnsupportedOperationException();
    }

    final public void set(int index, byte[] a) {
        throw new UnsupportedOperationException();
    }

    final public int add(byte[] a) {
        throw new UnsupportedOperationException();
    }

    final public int add(byte[] value, int off, int len) {
        throw new UnsupportedOperationException();
    }

    final public int add(DataInput in, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // NOP
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        // NOP
    }

}
