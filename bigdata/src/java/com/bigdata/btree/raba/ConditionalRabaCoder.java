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
 * Created on Aug 31, 2009
 */

package com.bigdata.btree.raba;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Iterator;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Coder conditionally applies other {@link IRabaCoder}s based on a condition,
 * typically the branching factor or the #of elements in the {@link IRaba}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConditionalRabaCoder implements IRabaCoder, Externalizable {

    private int bigSize;
    private IRabaCoder smallCoder;
    private IRabaCoder bigCoder;

    /**
     * Return <code>true</code> iff the "small" {@link IRabaCoder} should be
     * applied.
     * 
     * @param size The size of the {@link IRaba} to be coded.
     * 
     * @return
     */
    protected boolean isSmall(final int size) {
        
        return size < bigSize;
        
    }
    
    final public boolean isKeyCoder() {
    
        return smallCoder.isKeyCoder() && bigCoder.isKeyCoder();
        
    }

    final public boolean isValueCoder() {
        
        return smallCoder.isValueCoder() && bigCoder.isValueCoder();
        
    }
    
    /**
     * De-serialization ctor.
     */
    public ConditionalRabaCoder() {
        
    }

    /**
     * 
     * @param smallCoder
     *            The coder for a small {@link IRaba}.
     * @param bigCoder
     *            The coder for a large {@link IRaba}.
     * @param bigSize
     *            An {@link IRaba} with this many elements will be coded using
     *            the {@link #bigCoder}.
     */
    public ConditionalRabaCoder(final IRabaCoder smallCoder,
            final IRabaCoder bigCoder, final int bigSize) {

        final boolean isKeyCoder = smallCoder.isKeyCoder()
                && bigCoder.isKeyCoder();

        final boolean isValueCoder = smallCoder.isValueCoder()
                && bigCoder.isValueCoder();

        if (!isKeyCoder && !isValueCoder)
            throw new IllegalArgumentException();

        this.smallCoder = smallCoder;
        
        this.bigCoder = bigCoder;
        
        this.bigSize = bigSize;
        
    }

    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {

        final boolean isSmall = data.getByte(0) == 1 ? true : false;

        final AbstractFixedByteArrayBuffer delegateSlice = data
                .slice(1, data.len() - 1);

        final ICodedRaba codedRaba;
        if (isSmall) {

            codedRaba = smallCoder.decode(delegateSlice);

        } else {

            codedRaba = bigCoder.decode(delegateSlice);
            
        }

        // wraps coded raba to return the original data().
        return new CodedRabaSlice(data, codedRaba);

    }

    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        final int size = raba.size();

        final boolean isSmall = isSmall(size);

        final int O_origin = buf.pos();

        buf.putByte((byte) (isSmall ? 1 : 0));

        final ICodedRaba delegateCodedRaba;
        if (isSmall) {

            delegateCodedRaba = smallCoder.encodeLive(raba, buf);

        } else {

            delegateCodedRaba = bigCoder.encodeLive(raba, buf);

        }

        final AbstractFixedByteArrayBuffer delegateSlice = delegateCodedRaba
                .data();

        return new CodedRabaSlice(buf.slice(O_origin, delegateSlice.len() + 1),
                delegateCodedRaba);
        
    }

    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        final int size = raba.size();

        final boolean isSmall = isSmall(size);

        final int O_origin = buf.pos();
        
        buf.putByte((byte) (isSmall ? 1 : 0));

        final AbstractFixedByteArrayBuffer slice;
        if (isSmall) {

            slice = smallCoder.encode(raba, buf);

        } else {

            slice = bigCoder.encode(raba, buf);
            
        }

        return buf.slice(O_origin, slice.len() + 1);
        
    }

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }

        bigSize = (int) LongPacker.unpackLong(in);

        smallCoder = (IRabaCoder) in.readObject();

        bigCoder = (IRabaCoder) in.readObject();

    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        
        out.writeByte(VERSION0);
        
        LongPacker.packLong(out, bigSize);
        
        out.writeObject(smallCoder);
        
        out.writeObject(bigCoder);
        
    }
    
    private static final byte VERSION0 = 0x00;

    /**
     * Helper class used to wrap an {@link ICodedRaba} while returning the
     * caller's slice for the backing data. We use this to have the
     * {@link ConditionalRabaCoder} return the original slice, not the slice
     * after the first byte (which indicates whether to use the small or large
     * coder) has been chopped off.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class CodedRabaSlice implements ICodedRaba {
        
        private final IRaba delegate;

        private final AbstractFixedByteArrayBuffer data;

        CodedRabaSlice(final AbstractFixedByteArrayBuffer data,
                final IRaba delegate) {

            this.delegate = delegate;
            
            this.data = data;

        }

        public AbstractFixedByteArrayBuffer data() {
            
            return data;
            
        }
        
        public int add(byte[] value, int off, int len) {
            return delegate.add(value, off, len);
        }

        public int add(byte[] a) {
            return delegate.add(a);
        }

        public int add(DataInput in, int len) throws IOException {
            return delegate.add(in, len);
        }

        public int capacity() {
            return delegate.capacity();
        }

        public int copy(int index, OutputStream os) {
            return delegate.copy(index, os);
        }

        public byte[] get(int index) {
            return delegate.get(index);
        }

        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        public boolean isFull() {
            return delegate.isFull();
        }

        public boolean isKeys() {
            return delegate.isKeys();
        }

        public boolean isNull(int index) {
            return delegate.isNull(index);
        }

        public boolean isReadOnly() {
            return delegate.isReadOnly();
        }

        public Iterator<byte[]> iterator() {
            return delegate.iterator();
        }

        public int length(int index) {
            return delegate.length(index);
        }

        public int search(byte[] searchKey) {
            return delegate.search(searchKey);
        }

        public void set(int index, byte[] a) {
            delegate.set(index, a);
        }

        public int size() {
            return delegate.size();
        }
        
    }
    
}
