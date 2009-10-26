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

import com.bigdata.btree.raba.AbstractRaba;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Useful when a B+Tree uses keys but not values. The coder maintains the
 * {@link IRaba#size()}, but any <code>byte[]</code> values stored under the
 * B+Tree will be <strong>discarded</strong> by this {@link IRabaCoder}.
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
     
        return true;
        
    }

    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        if (raba == null)
            throw new IllegalArgumentException();

        if (raba.isKeys()) {

            // not allowed for B+Tree keys.
            throw new UnsupportedOperationException();

        }

        final int O_origin = buf.pos();

        final int size = raba.size();

        buf.putInt(size);

        return new EmptyCodedRaba(buf.slice(O_origin, buf.pos() - O_origin),
                size);

    }

    /**
     * <strong>Any data in the {@link IRaba} will be discarded!</strong> Only
     * the {@link IRaba#size()} is maintained.
     */
    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        if (raba == null)
            throw new IllegalArgumentException();

        if (raba.isKeys()) {

            // not allowed for B+Tree keys.
            throw new UnsupportedOperationException();
            
        }

        final int O_origin = buf.pos();
        
        buf.putInt(raba.size());

        return buf.slice(O_origin, buf.pos() - O_origin);

    }

    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {
        
        return new EmptyCodedRaba(data);
        
    }

    /**
     * An {@link ICodedRaba} for use when the encoded logical byte[][] was
     * empty.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class EmptyCodedRaba implements ICodedRaba {
        
        private final AbstractFixedByteArrayBuffer data;
        
        private final int size;
        
        public EmptyCodedRaba(final AbstractFixedByteArrayBuffer data) {

            if (data == null)
                throw new IllegalArgumentException();
            
            this.data = data;

            size = data.getInt(0);
            
        }
        
        public EmptyCodedRaba(final AbstractFixedByteArrayBuffer data, final int size) {

            if (data == null)
                throw new IllegalArgumentException();
            
            this.data = data;

            this.size = size;
            
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
        
            return size;
            
        }

        final public int size() {
            
            return size;
            
        }
        
        final public boolean isEmpty() {
            
            return size == 0;
            
        }

        final public boolean isFull() {
            
            return true;
            
        }

        final public boolean isNull(int index) {
            
            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();
            
            return true;
            
        }

        final public int length(int index) {
            
            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();
            
            throw new NullPointerException();
            
        }

        final public byte[] get(int index) {
            
            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();
            
            return null;
            
        }

        final public int copy(int index, OutputStream os) {
        
            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();

            throw new NullPointerException();
        
        }

        final public Iterator<byte[]> iterator() {

            return new Iterator<byte[]>() {

                int i = 0;

                public boolean hasNext() {

                    return i < size;

                }

                public byte[] next() {

                    i++;
                    
                    return null;
                    
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
