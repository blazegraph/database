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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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
 * 
 * FIXME Unit tests for this class.
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
    protected boolean isSmall(int size) {
        
        return size < bigSize;
        
    }
    
    final public boolean isKeyCoder() {
    
        return smallCoder.isKeyCoder() && bigCoder.isKeyCoder();
        
    }

    final public boolean isValueCoder() {
        
        return smallCoder.isValueCoder() && bigCoder.isValueCoder();
        
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

        final AbstractFixedByteArrayBuffer slice = data
                .slice(1, data.len() - 1);

        if (isSmall) {

            return smallCoder.decode(slice);

        }

        return bigCoder.decode(slice);

    }

    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        final int size = raba.size();

        final boolean isSmall = isSmall(size);

        buf.putByte((byte) (isSmall ? 1 : 0));

        if (isSmall) {

            return smallCoder.encode(raba, buf);

        }

        return bigCoder.encode(raba, buf);

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }

        bigSize = (int) LongPacker.unpackLong(in);

        smallCoder = (IRabaCoder) smallCoder;

        bigCoder = (IRabaCoder) bigCoder;

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        out.writeByte(VERSION0);
        
        LongPacker.packLong(out, bigSize);
        
        out.writeObject(smallCoder);
        
        out.writeObject(bigCoder);
        
    }
    
    private static final byte VERSION0 = 0x00;

}
