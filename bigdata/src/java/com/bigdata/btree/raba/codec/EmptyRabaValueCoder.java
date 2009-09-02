/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Aug 26, 2009
 */

package com.bigdata.btree.raba.codec;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.raba.AbstractRaba;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;

/**
 * Useful when a B+Tree uses keys but not values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyRabaValueCoder implements IRabaCoder, Externalizable {

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP
        
    }

    public static transient final EmptyRabaValueCoder INSTANCE = new EmptyRabaValueCoder();
    
    public EmptyRabaValueCoder() {
        
    }
    
    /**
     * No.  Keys can not be constrained to be empty.
     */
    final public boolean isKeyCoder() {
        return false;
    }

    /**
     * Yes.
     */
    final public boolean isValueCoder() {
        return false;
    }

    /**
     * @throws UnsupportedOperationException
     *             unless {@link IRaba#isEmpty()} is <code>false</code>.
     */
    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        if (raba == null)
            throw new IllegalArgumentException();

        if (!raba.isEmpty())
            throw new UnsupportedOperationException();

        return FixedByteArrayBuffer.EMPTY;

    }

    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {
        
        return new EmptyRabaValueDecoder(data);
        
    }

    /**
     * An {@link ICodedRaba} for use when the encoded logical byte[][] was
     * empty.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class EmptyRabaValueDecoder implements ICodedRaba {

//        public static final EmptyRabaDecoder INSTANCE = new EmptyRabaDecoder();
        
        private final AbstractFixedByteArrayBuffer data;
        
        public EmptyRabaValueDecoder(final AbstractFixedByteArrayBuffer data) {

            if (data == null)
                throw new IllegalArgumentException();
            
            this.data = data;
            
        }
        
        final public AbstractFixedByteArrayBuffer data() {
     
            return data;
            
        }

        /**
         * Yes.
         */
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

}
