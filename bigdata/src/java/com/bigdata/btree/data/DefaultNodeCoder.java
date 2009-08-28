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
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.MutableNodeData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Default implementation for immutable {@link INodeData} records.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo partly mutable coded records for {@link INodeData} are feasible. The
 *       only reason to expand an {@link INodeData} into a fully
 *       {@link MutableNodeData} is if we need to modify the keys. The rest of
 *       the fields could be easily patched in place if they were not coded
 *       (they are fixed length fields). Of course, we have a more compact
 *       representation when those fields ARE coded.
 */
public class DefaultNodeCoder implements IAbstractNodeCoder<INodeData>,
        Externalizable {

    private IRabaCoder keysCoder;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        keysCoder = (IRabaCoder) in.readObject();
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.writeObject(keysCoder);
        
    }

    /** No. */
    final public boolean isLeafCoder() {
        
        return false;
        
    }

    /** Yes. */
    public boolean isNodeCoder() {

        return true;
        
    }

    public String toString() {

        return super.toString() + "{keysCoder=" + keysCoder + "}";

    }
    
    /**
     * 
     * @param keysCoder
     *            The {@link IRabaCoder} for the node's keys.
     */
    public DefaultNodeCoder(final IRabaCoder keysCoder) {

        if (keysCoder == null)
            throw new IllegalArgumentException();

        this.keysCoder = keysCoder;

    }

    public INodeData decode(final AbstractFixedByteArrayBuffer data) {

        return new ReadOnlyNodeData(data, keysCoder);
        
    }

    public AbstractFixedByteArrayBuffer encode(final INodeData node,
            final DataOutputBuffer buf) {

        if (node == null)
            throw new IllegalArgumentException();

        if (keysCoder == null)
            throw new IllegalArgumentException();

        if (buf == null)
            throw new IllegalArgumentException();

        // cache some fields.
        final int nkeys = node.getKeyCount();
        final int nentries = node.getSpannedTupleCount();

        // The byte offset of the start of the coded data in the buffer.
        final int O_origin = buf.pos();
        
        buf.putByte(ReadOnlyNodeData.NODE);

        buf.putShort(ReadOnlyNodeData.VERSION0);
        
        buf.putShort((short) 0/* flags */);
        
        buf.putInt(nkeys); // @todo pack?
        
        buf.putInt(nentries); // @todo pack?

        final int O_keysSize = buf.pos();
        buf.skip(ReadOnlyNodeData.SIZEOF_KEYS_SIZE);
//            buf.putInt(encodedKeys.length); // keySize @todo pack?
        
        // childAddr[] : @todo code childAddr[]
        final int O_childAddr = buf.pos();
        for (int i = 0; i <= nkeys; i++) {
            
            buf.putLong(node.getChildAddr(i));
            
        }
        
        // childEntryCount[] : @todo code childEntryCount[]
        final int O_childEntryCount = buf.pos();
        for (int i = 0; i <= nkeys; i++) {
            
            buf.putInt(node.getChildEntryCount(i));
            
        }
        
        // write the encoded keys on the buffer.
        final int O_keys = buf.pos();
//            encodedKeys.limit(encodedKeys.capacity());
//            encodedKeys.rewind();
//          assert b.position() == b.limit();
//            buf.put(encodedKeys);

        final AbstractFixedByteArrayBuffer encodedKeys = keysCoder.encode(node
                .getKeys(), buf);

//            this.keys = keysCoder.decode(encodedKeys);
        
        // Patch the byte length of the coded keys on the buffer.
        buf.putInt(O_keysSize, encodedKeys.len());

//            // prepare buffer for writing on the store [limit := pos; pos : =0] 
//            b.flip();
        
        // save read-only reference to the buffer.
//            this.b = b.asReadOnlyBuffer();
//            this.b = FixedByteArrayBuffer.wrap(buf.toByteArray());
        
        return buf.slice(O_origin, buf.pos() - O_origin);
        
    }

    /**
     * A read-only view of the data for a B+Tree node.
     * <p>
     * Note: The leading byte of the record format codes for a leaf, a double-linked
     * leaf or a node in a manner which is compatible with {@link ReadOnlyNodeData}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class ReadOnlyNodeData extends AbstractReadOnlyNodeData<INodeData>
            implements INodeData {
        
        /** The backing buffer */
        private final AbstractFixedByteArrayBuffer b;
        
        // fields which are cached by the ctor.
        private final int nkeys;
        private final int nentries;

        /**
         * Offset of the encoded childAddr[] in the buffer.
         */
        private final int O_childAddr;
        
        /**
         * Offset of the encoded childEntryCount[] in the buffer.
         */
        private final int O_childEntryCount;

        /**
         * Offset of the encoded keys in the buffer.
         */
        private final int O_keys;

        private final IRaba keys;

        public final AbstractFixedByteArrayBuffer buf() {

            return b;
            
        }
        
        /**
         * Decode in place (wraps a buffer containing an encoded node data record).
         * 
         * @param b
         *            The buffer containing the data for the node.
         */
        public ReadOnlyNodeData(final AbstractFixedByteArrayBuffer buf,
                final IRabaCoder keysCoder) {

            if (buf == null)
                throw new IllegalArgumentException();

            if (keysCoder == null)
                throw new IllegalArgumentException();

            int pos = O_TYPE;
            final byte type = buf.getByte(pos);
            pos += SIZEOF_TYPE;

            switch (type) {
            case NODE:
                break;
            case LEAF:
                throw new AssertionError();
            case LINKED_LEAF:
                throw new AssertionError();
            default:
                throw new AssertionError("type=" + type);
            }

            final int version = buf.getShort(pos);
            pos += SIZEOF_VERSION;
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new AssertionError("version=" + version);
            }
            
            // skip over flags (they are unused for a node) : @todo version timestamps flag.
            pos += SIZEOF_FLAGS;
            
            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;
            
            this.nentries = buf.getInt(pos);
            pos += SIZEOF_ENTRY_COUNT;
            
            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

            O_childAddr = pos;
            
            O_childEntryCount = O_childAddr + (nkeys + 1) * SIZEOF_ADDR;

            O_keys = O_childEntryCount + (nkeys + 1) * SIZEOF_ENTRY_COUNT;
//            b.position(O_keys);
//            b.limit(b.position() + keysSize);
            this.keys = keysCoder.decode(buf.slice(O_keys, keysSize));
//            assert b.position() == O_keys + keysSize;
            
            // save reference to buffer
//            this.b = (b.isReadOnly() ? b : b.asReadOnlyBuffer());
            this.b = buf;

        }

//        /**
//         * Encode the node data.
//         * 
//         * @param data
//         *            The data to be encoded.
//         * @param buf
//         *            The buffer on which the coded representation will be written.
//         */
//        public ReadOnlyNodeData(final INodeData node, final IRabaCoder keysCoder,
//                DataOutputBuffer buf) {
//
//            if (node == null)
//                throw new IllegalArgumentException();
//
//            if (keysCoder == null)
//                throw new IllegalArgumentException();
//
//            if (buf == null)
//                throw new IllegalArgumentException();
//
//            // cache some fields.
//            this.nkeys = node.getKeyCount();
//            this.nentries = node.getSpannedTupleCount();
//
//            // encode the keys.
////            this.keys = keysCoder.encode(node.getKeys());
////            final ByteBuffer encodedKeys = ((IRabaDecoder) keys).data();
////            final byte[] encodedKeys = keysCoder.encode(node.getKeys(), buf);
////            this.keys = keysCoder.decode(FixedByteArrayBuffer.wrap(encodedKeys));
//
////            // figure out how the size of the buffer (exact fit).
////            final int capacity = //
////                    SIZEOF_TYPE + //
////                    SIZEOF_VERSION + //
////                    SIZEOF_FLAGS + //
////                    SIZEOF_NKEYS + //
////                    SIZEOF_ENTRY_COUNT + //
////                    Bytes.SIZEOF_INT + // keysSize
////                    SIZEOF_ADDR * (nkeys + 1) + // childAddr[]
////                    SIZEOF_ENTRY_COUNT * (nkeys + 1) + // childEntryCount[]
////                    encodedKeys.length // keys
////            ;
//            
////            final ByteBuffer b = ByteBuffer.allocate(capacity);
////            final ByteArrayBuffer b = buf; // alias.
////            b.ensureCapacity(capacity);
////            b.reset();
//
//            // The byte offset of the start of the coded data in the buffer.
//            final int O_origin = buf.pos();
//            
//            buf.putByte(NODE);
//
//            buf.putShort(VERSION0);
//            
//            buf.putShort((short) 0/* flags */);
//            
//            buf.putInt(nkeys); // @todo pack?
//            
//            buf.putInt(nentries); // @todo pack?
//
//            final int O_keysSize = buf.pos();
//            buf.skip(SIZEOF_KEYS_SIZE);
////            buf.putInt(encodedKeys.length); // keySize @todo pack?
//            
//            // childAddr[] : @todo code childAddr[]
//            O_childAddr = buf.pos();
//            for (int i = 0; i <= nkeys; i++) {
//                
//                buf.putLong(node.getChildAddr(i));
//                
//            }
//            
//            // childEntryCount[] : @todo code childEntryCount[]
//            O_childEntryCount = buf.pos();
//            for (int i = 0; i <= nkeys; i++) {
//                
//                buf.putInt(node.getChildEntryCount(i));
//                
//            }
//            
//            // write the encoded keys on the buffer.
//            O_keys = buf.pos();
////            encodedKeys.limit(encodedKeys.capacity());
////            encodedKeys.rewind();
////          assert b.position() == b.limit();
////            buf.put(encodedKeys);
//
//            final AbstractFixedByteArrayBuffer encodedKeys = keysCoder.encode(node
//                    .getKeys(), buf);
//
//            this.keys = keysCoder.decode(encodedKeys);
//            
//            // Patch the byte length of the coded keys on the buffer.
//            buf.putInt(O_keysSize, encodedKeys.len());
//
////            // prepare buffer for writing on the store [limit := pos; pos : =0] 
////            b.flip();
//            
//            // save read-only reference to the buffer.
////            this.b = b.asReadOnlyBuffer();
////            this.b = FixedByteArrayBuffer.wrap(buf.toByteArray());
//            this.b = buf.slice(O_origin, buf.pos() - O_origin);
//            
//        }

        /**
         * Always returns <code>false</code>.
         */
        final public boolean isLeaf() {

            return false;

        }

        /**
         * Yes.
         */
        final public boolean isReadOnly() {
            
            return true;
            
        }
        
        /**
         * {@inheritDoc}. This field is cached.
         */
        final public int getKeyCount() {
            
            return nkeys;
            
        }

        /**
         * {@inheritDoc}. This field is cached.
         */
        final public int getChildCount() {
            
            return nkeys + 1;
            
        }

        /**
         * {@inheritDoc}. This field is cached.
         */
        final public int getSpannedTupleCount() {
            
            return nentries;
            
        }

        /**
         * Bounds check.
         * 
         * @throws IndexOutOfBoundsException
         *             if <i>index</i> is LT ZERO (0)
         * @throws IndexOutOfBoundsException
         *             if <i>index</i> is GE <i>nkeys</i>
         */
        protected boolean assertChildIndex(final int index) {
            
            if (index < 0 || index > nkeys)
                throw new IndexOutOfBoundsException();
            
            return true;
            
        }

        final public long getChildAddr(final int index) {

            assert assertChildIndex(index);

            return b.getLong(O_childAddr + index * SIZEOF_ADDR);

        }

        final public int getChildEntryCount(final int index) {

            assert assertChildIndex(index);

            return b.getInt(O_childEntryCount + index * SIZEOF_ENTRY_COUNT);

        }

        final public IRaba getKeys() {

            return keys;

        }

        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getName() + "{");

            DefaultNodeCoder.toString(this, sb);

            sb.append("}");
            
            return sb.toString();
            
        }

    }

    /**
     * Utility method formats the {@link INodeData}.
     * 
     * @param node
     *            A node data record.
     * @param sb
     *            The representation will be written onto this object.
     * 
     * @return The <i>sb</i> parameter.
     */
    static public StringBuilder toString(final INodeData node,
            final StringBuilder sb) {

        final int nchildren = node.getChildCount();

        sb.append(", nchildren=" + nchildren);

        sb.append(", spannedTupleCount=" + node.getSpannedTupleCount());

        sb.append(", keys=" + node.getKeys());

        {

            sb.append(", childAddr=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildAddr(i));

            }

            sb.append("]");

        }

        {

            sb.append(", childEntryCount=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildEntryCount(i));

            }

            sb.append("]");

        }

        return sb;

    }

}
