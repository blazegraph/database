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
public class DefaultNodeCoder implements IAbstractNodeDataCoder<INodeData>,
        Externalizable {

    protected static final byte VERSION0 = 0x00;
    private IRabaCoder keysCoder;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();
        switch(version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }
        
        keysCoder = (IRabaCoder) in.readObject();
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(VERSION0);

        out.writeObject(keysCoder);
        
    }

    /** No. */
    final public boolean isLeafDataCoder() {
        
        return false;
        
    }

    /** Yes. */
    public boolean isNodeDataCoder() {

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

        final boolean hasVersionTimestamps =node.hasVersionTimestamps();
        short flags = 0;
        if (hasVersionTimestamps) {
            flags |= AbstractReadOnlyNodeData.FLAG_VERSION_TIMESTAMPS;
        }

        buf.putShort(flags);

//        @todo pack (must unpack in decode).
        buf.putInt(nkeys);
        buf.putInt(nentries);
//        try {
//
//            buf.packLong(nkeys);
//
//            buf.packLong(nentries);
//            
//        } catch (IOException ex) {
//         
//            throw new RuntimeException(ex);
//            
//        }

        // The offset at which the byte length of the keys will be recorded.
        final int O_keysSize = buf.pos();
        buf.skip(ReadOnlyNodeData.SIZEOF_KEYS_SIZE);
        
        // Write the encoded keys on the buffer.
        final int O_keys = buf.pos();
        final AbstractFixedByteArrayBuffer encodedKeys = keysCoder.encode(node
                .getKeys(), buf);

        // Patch the byte length of the coded keys on the buffer.
        buf.putInt(O_keysSize, encodedKeys.len());

        // childAddr[] : @todo code childAddr[] (needs IAddressManager if store aware coding).
        final int O_childAddr = buf.pos();
        for (int i = 0; i <= nkeys; i++) {
            
            buf.putLong(node.getChildAddr(i));
            
        }
        
        // childEntryCount[] : @todo code childEntryCount[]
        final int O_childEntryCount = buf.pos();
        for (int i = 0; i <= nkeys; i++) {
            
            buf.putInt(node.getChildEntryCount(i));
            
        }
        
        if(hasVersionTimestamps) {
            
            buf.putLong(node.getMinimumVersionTimestamp());

            buf.putLong(node.getMaximumVersionTimestamp());
            
        }
        
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
        private final short flags;
        private final int nkeys;
        private final int nentries;

        /**
         * Offset of the encoded keys in the buffer.
         */
        private final int O_keys;

        private final IRaba keys;

        /**
         * Offset of the encoded childAddr[] in the buffer.
         */
        private final int O_childAddr;
        
        /**
         * Offset of the encoded childEntryCount[] in the buffer.
         */
        private final int O_childEntryCount;
                
        final public AbstractFixedByteArrayBuffer data() {
            
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
            
            // flags
            flags = buf.getShort(pos);
            pos += SIZEOF_FLAGS;

            // @todo unpack - must wrap buf as DataInputStream for this, so updating internal pos.
            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;
            
            // @todo unpack - must wrap buf as DataInputStream for this.
            this.nentries = buf.getInt(pos);
            pos += SIZEOF_ENTRY_COUNT;
            
            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

//          O_keys = O_childEntryCount + (nkeys + 1) * SIZEOF_ENTRY_COUNT;
            O_keys = pos;
            this.keys = keysCoder.decode(buf.slice(O_keys, keysSize));
            pos += keysSize;
//            assert b.position() == O_keys + keysSize;
            
            O_childAddr = pos;
            
            O_childEntryCount = O_childAddr + (nkeys + 1) * SIZEOF_ADDR;

            // save reference to buffer
//            this.b = (b.isReadOnly() ? b : b.asReadOnlyBuffer());
            this.b = buf;

        }

        /**
         * The offset into the buffer of the minimum version timestamp, which is
         * an int64 field. The maximum version timestamp is the next field. This
         * offset is computed dynamically to keep down the size of the node
         * object in memory.
         */
        private int getVersionTimestampOffset() {

            return O_childEntryCount + ((nkeys + 1) * SIZEOF_ENTRY_COUNT);
            
        }

        final public boolean hasVersionTimestamps() {
            
            return ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
            
        }

        final public long getMinimumVersionTimestamp() {
            
            if(!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            final int off = getVersionTimestampOffset();
            
            // at the offset.
            return b.getLong(off);
            
        }

        final public long getMaximumVersionTimestamp() {
            
            if(!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            final int off = getVersionTimestampOffset() + SIZEOF_TIMESTAMP;

            // one long value beyond the offset.
            return b.getLong(off);

        }

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
         * Yes.
         */
        final public boolean isCoded() {
            
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
         *             if <i>index</i> is GT <i>nkeys+1</i>
         */
        protected boolean assertChildIndex(final int index) {
            
            if (index < 0 || index > nkeys + 1)
                throw new IndexOutOfBoundsException("index=" + index
                        + ", nkeys=" + nkeys);

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

        if(node.hasVersionTimestamps()) {
            
            sb.append(", versionTimestamps={min="
                    + node.getMinimumVersionTimestamp() + ",max="
                    + node.getMaximumVersionTimestamp() + "}");

        }
        
        return sb;

    }

}

    /*
     * @todo old code from NodeSerializer.
     */
    
//    private void putChildAddresses(IAddressManager addressManager,
//            DataOutputBuffer os, INodeData node) throws IOException {
//
//        final int nchildren = node.getChildCount();
//
//        for (int i = 0; i < nchildren; i++) {
//
//            final long addr = node.getChildAddr(i);
//
//            /*
//             * Children MUST have assigned persistent identity.
//             */
//            if (addr == 0L) {
//
//                throw new RuntimeException("Child is not persistent: index="
//                        + i);
//
//            }
//
//            os.writeLong(addr);
//
//        }
//
//    }
//
//    private void getChildAddresses(IAddressManager addressManager,
//            DataInput is, long[] childAddr, int nchildren) throws IOException {
//
//        for (int i = 0; i < nchildren; i++) {
//
//            final long addr = is.readLong();
//
//            if (addr == 0L) {
//
//                throw new RuntimeException(
//                        "Child does not have persistent address: index=" + i);
//
//            }
//
//            childAddr[i] = addr;
//
//        }
//
//    }
//
//    /**
//     * Write out a packed array of the #of entries spanned by each child of some
//     * node.
//     * 
//     * @param os
//     *            The output stream.
//     * @param childEntryCounts
//     *            The #of entries spanned by each direct child.
//     * @param nchildren
//     *            The #of elements of that array that are defined.
//     * 
//     * @throws IOException
//     * 
//     * @todo customizable serializer interface configured in
//     *       {@link IndexMetadata}.
//     */
//    static protected void putChildEntryCounts(final DataOutput os,
//            final INodeData node) throws IOException {
//
//        final int nchildren = node.getChildCount();
//
//        for (int i = 0; i < nchildren; i++) {
//
//            final long nentries = node.getChildEntryCount(i);
//
//            /*
//             * Children MUST span some entries.
//             */
//            if (nentries == 0L) {
//
//                throw new RuntimeException(
//                        "Child does not span entries: index=" + i);
//
//            }
//
//            LongPacker.packLong(os, nentries);
//
//        }
//
//    }
//
//    /**
//     * Read in a packed array of the #of entries spanned by each child of some
//     * node.
//     * 
//     * @param is
//     * @param childEntryCounts
//     *            The #of entries spanned by each direct child.
//     * @param nchildren
//     *            The #of elements of that array that are defined.
//     * @throws IOException
//     * 
//     * @todo customizable serializer interface configured in
//     *       {@link IndexMetadata}.
//     */
//    static protected void getChildEntryCounts(DataInput is,
//            int[] childEntryCounts, int nchildren) throws IOException {
//
//        for (int i = 0; i < nchildren; i++) {
//
//            final int nentries = (int) LongPacker.unpackLong(is);
//
//            if (nentries == 0L) {
//
//                throw new RuntimeException(
//                        "Child does not span entries: index=" + i);
//
//            }
//
//            childEntryCounts[i] = nentries;
//
//        }
//
//    }
