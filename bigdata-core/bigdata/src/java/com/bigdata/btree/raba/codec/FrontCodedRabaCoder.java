/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.btree.raba.codec;

import it.unimi.dsi.fastutil.bytes.ByteArrayFrontCodedList;
import it.unimi.dsi.fastutil.bytes.custom.CustomByteArrayFrontCodedList;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.util.Bytes;

/**
 * Class provides (de-)compression for logical byte[][]s based on front coding.
 * The data MUST be ordered. <code>null</code> values are not allowed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FrontCodedRabaCoder implements IRabaCoder, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 4943035649252818747L;
    
    protected static final Logger log = Logger
            .getLogger(FrontCodedRabaCoder.class);

    private int ratio;

    @Override
    public String toString() {

        return super.toString() + "{ratio=" + ratio + "}";
        
    }

    /**
     * A pre-parameterized version of the {@link FrontCodedRabaCoder} which is
     * used as the default {@link IRabaCoder} for B+Tree keys for both nodes and
     * leaves.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class DefaultFrontCodedRabaCoder extends FrontCodedRabaCoder {

        /**
         * 
         */
        private static final long serialVersionUID = 7300378339686013560L;
        
        public static final transient DefaultFrontCodedRabaCoder INSTANCE = new DefaultFrontCodedRabaCoder();
        
        protected transient static final int DEFAULT_RATIO = 8;
        
        public DefaultFrontCodedRabaCoder() {

            super(DEFAULT_RATIO);
            
        }
        
        @Override
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            // NOP
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            // NOP
        }

    }

    /**
     * De-serialization ctor.
     */
    public FrontCodedRabaCoder() {
    }
    
    /**
     * @param ratio
     *            The ratio as defined by {@link ByteArrayFrontCodedList}. For
     *            front-coding, compression trades directly for search
     *            performance. Every ratio byte[]s is fully coded. Binary search
     *            is used on the fully coded byte[]s and will identify a bucket
     *            <i>ratio</i> front-coded values. Linear search is then
     *            performed within the bucket of front-coded values in which the
     *            key would be found if it is present. Therefore the ratio is
     *            also the maximum of steps in the linear scan.
     *            <p>
     *            Let <code>m := n / ratio</code>, where <i>n</i> is the #of
     *            entries in the <code>byte[][]</code> (the size of the total
     *            search problem), <i>m</i> is the size of the binary search
     *            problem and ratio is the size of the linear search problem.
     *            Solving for ratio, we have: <code>ratio := n / m</code>. Some
     *            examples:
     * 
     *            <pre>
     * m = n(64)/ratio(16) = 4
     * 
     * m = n(64)/ratio(8) = 8
     * 
     * m = n(64)/ratio(6) &tilde; 11
     * 
     * m = n(64)/ratio(4) = 16
     * </pre>
     */
    public FrontCodedRabaCoder(final int ratio) {

        this.ratio = ratio;

    }

    @Override
    final public boolean isKeyCoder() {

        return true;

    }

    @Override
    final public boolean isValueCoder() {

        return false;

    }

    @Override
    public boolean isDuplicateKeys() {

        return false;
        
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        ratio = in.readInt();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeInt(ratio);

    }

    private static final byte VERSION0 = 0x00;

    private static final int SIZEOF_VERSION = Bytes.SIZEOF_BYTE;

    private static final int SIZEOF_SIZE = Bytes.SIZEOF_INT;

    private static final int SIZEOF_RATIO = Bytes.SIZEOF_INT;

    /** The byte offset of the version identifier. */
    private static final int O_VERSION = 0;

    /**
     * The byte offset of the field coding the #of entries in the logical
     * byte[][].
     */
    private static final int O_SIZE = O_VERSION + SIZEOF_VERSION;

    /** The byte offset of the field coding the ratio. */
    private static final int O_RATIO = O_SIZE + SIZEOF_SIZE;

    /** The byte offset of the start of the front-coded representation. */
    private static final int O_DATA = O_RATIO + SIZEOF_RATIO;

    @Override
    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        if (raba == null)
            throw new IllegalArgumentException();

        if (!raba.isKeys())
            throw new UnsupportedOperationException("Must be keys.");

        if (buf == null)
            throw new IllegalArgumentException();

        final int size = raba.size();

        if (log.isInfoEnabled())
            log.info("n=" + raba.size() + ", capacity=" + raba.capacity()
                    + ", ratio=" + ratio);

        // The byte offset of the origin of the coded record into the buffer.
        final int O_origin = buf.pos();

        // front-code the byte[][].
        final CustomByteArrayFrontCodedList decoder = new CustomByteArrayFrontCodedList(
                raba.iterator(), ratio, isDuplicateKeys());

        try {

            // The record version identifier.
            buf.write(VERSION0);

            // #of entries (zero length indicates NO data)
            buf.writeInt(size);

            // The ratio used to front code the data.
            buf.writeInt(ratio);

            decoder.getBackingBuffer().writeOn(buf);

            buf.flush();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

        return new CodedRabaImpl(slice, decoder);
        
    }

    @Override
    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        /*
         * Note: there is nearly zero overhead associated with this code path.
         * The only unnecessary action is wrapping the CodedRabaImpl with the
         * (slice,decoder), which is pretty near zero cost.
         */
        
        return encodeLive(raba, buf).data();
        
    }

    @Override
    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {

        return new CodedRabaImpl(data, isDuplicateKeys());

    }

    /**
     * Decoder for an ordered logical byte[][] without <code>null</code>s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class CodedRabaImpl extends AbstractCodedRaba {

        private final AbstractFixedByteArrayBuffer data;

        private final CustomByteArrayFrontCodedList decoder;

        /**
         * 
         * @param data
         *            The record containing the coded data.
         * @param hasDups
         *            <code>true</code> iff the {@link IRabaCoder} supports
         *            duplicate keys.
         */
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data,
                final boolean hasDups) {

            final byte version = data.getByte(O_VERSION);

            if (version != VERSION0) {

                throw new RuntimeException("Unknown version: " + version);

            }
            
            // The #of entries in the logical byte[][].
            final int size = data.getInt(O_SIZE);

            // The ratio.
            final int ratio = data.getInt(O_RATIO);

            // wrap slice with decoder.
            this.decoder = new CustomByteArrayFrontCodedList(size, ratio, data
                    .array(), data.off() + O_DATA, data.len(), hasDups);

            this.data = data;

        }

        /**
         * Alternative constructor avoids the cost of constructing the decoder
         * when it is already available.
         * 
         * @param data
         *            The record containing the coded data.
         * @param decoder
         *            The decoder object.
         */
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data,
                final CustomByteArrayFrontCodedList decoder) {

            this.data = data;

            this.decoder = decoder;

        }

        @Override
        public AbstractFixedByteArrayBuffer data() {

            return data;

        }

        /**
         * Represents B+Tree keys.
         */
        @Override
        final public boolean isKeys() {

            return true;

        }

        @Override
        final public int size() {

            return decoder.size();

        }

        @Override
        final public int capacity() {

            return decoder.size();

        }

        @Override
        final public boolean isEmpty() {

            return size() == 0;

        }

        /**
         * Always returns <code>true</code> since the front-coded representation
         * is dense.
         */
        @Override
        final public boolean isFull() {

            return true;

        }

        /**
         * Always returns <code>false</code> (<code>null</code>s are not
         * allowed).
         */
        @Override
        final public boolean isNull(final int index) {

            return false;

        }

        @Override
        final public byte[] get(final int index) {

            return decoder.get(index);

        }

        @Override
        final public int length(final int index) {

            return decoder.arrayLength(index);

        }

        @Override
        public int copy(final int index, final OutputStream os) {

            try {

                return decoder.writeOn(os, index);

            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

        }

        @Override
        public Iterator<byte[]> iterator() {

            return decoder.iterator();

        }

        @Override
        public int search(final byte[] searchKey) {

            // optimization: always keys.
//            if(isKeys()) {
            
                return decoder.search(searchKey);
                
//            }
//            
//            throw new UnsupportedOperationException();

        }

    }

}
