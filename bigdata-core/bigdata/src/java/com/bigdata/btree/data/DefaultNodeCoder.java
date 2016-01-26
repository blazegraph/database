/*

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
/*
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.data;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.MutableNodeData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

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

    /**
     * 
     */
    private static final long serialVersionUID = 3998574101917337169L;

	/**
	 * The initial version of the serialized representation of the
	 * {@link DefaultNodeCoder} class (versus the serializer representation of
	 * the node or leaf).
	 */
    private final static transient byte VERSION0 = 0x00;
    
    private IRabaCoder keysCoder;

    @Override
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

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(VERSION0);

        out.writeObject(keysCoder);
        
    }

    /** No. */
    @Override
    final public boolean isLeafDataCoder() {
        
        return false;
        
    }

    /** Yes. */
    @Override
    public boolean isNodeDataCoder() {

        return true;
        
    }

    @Override
    public String toString() {

        return super.toString() + "{keysCoder=" + keysCoder + "}";

    }
    
    /**
     * De-serialization ctor.
     */
    public DefaultNodeCoder() {
        
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

    @Override
    public INodeData decode(final AbstractFixedByteArrayBuffer data) {

        return new ReadOnlyNodeData(data, keysCoder);
        
    }

    @Override
    public INodeData encodeLive(final INodeData node, final DataOutputBuffer buf) {

        if (node == null)
            throw new IllegalArgumentException();

        if (keysCoder == null)
            throw new IllegalArgumentException();

        if (buf == null)
            throw new IllegalArgumentException();

        final short version = ReadOnlyNodeData.currentVersion;
        
        // cache some fields.
        final int nkeys = node.getKeyCount();
        final long nentries = node.getSpannedTupleCount();

        // The byte offset of the start of the coded data in the buffer.
        final int O_origin = buf.pos();
        
        buf.putByte(ReadOnlyNodeData.NODE);

        buf.putShort(version);

		final boolean hasVersionTimestamps = node.hasVersionTimestamps();
		short flags = 0;
		if (hasVersionTimestamps) {
			flags |= AbstractReadOnlyNodeData.FLAG_VERSION_TIMESTAMPS;
		}

        buf.putShort(flags);

		buf.putInt(nkeys);

		if (nentries < 0) {
			/*
			 * Note: This allows ZERO entries in order to support some unit
			 * tests an empty node. However, an empty node is not a legal
			 * data structure in a btree. Only the root leaf may be empty.
			 */
			throw new RuntimeException();
		}
		if (version == ReadOnlyNodeData.VERSION0) {
			if (nentries > Integer.MAX_VALUE)
				throw new UnsupportedOperationException();
			buf.putInt((int) nentries);
		} else {
			buf.putLong(nentries);
		}

        // The offset at which the byte length of the keys will be recorded.
        final int O_keysSize = buf.pos();
        buf.skip(ReadOnlyNodeData.SIZEOF_KEYS_SIZE);
        
        // Write the encoded keys on the buffer.
//        final int O_keys = buf.pos();
        final ICodedRaba encodedKeys = keysCoder
                .encodeLive(node.getKeys(), buf);
//        final AbstractFixedByteArrayBuffer encodedKeysData = encodedKeys.data();

        // Patch the byte length of the coded keys on the buffer.
        buf.putInt(O_keysSize, encodedKeys.data().len());

        // childAddr[] : @todo code childAddr[] (needs IAddressManager if store aware coding).
//        final int O_childAddr = buf.pos();
        for (int i = 0; i <= nkeys; i++) {

            /*
             * See #855 (Child identity is not persistent).
             */
            final long childAddr = node.getChildAddr(i);
            
            if (childAddr == IRawStore.NULL)
                throw new AssertionError("Child is not persistent: index=" + i
                        + " out of " + nkeys + " entries, " + node.toString());
            
            buf.putLong(childAddr);
            
        }
        
//        final int O_childEntryCount = buf.pos();
		if (version == ReadOnlyNodeData.VERSION0) {
			long sum = 0;
			for (int i = 0; i <= nkeys; i++) {
				final long nchildren = node.getChildEntryCount(i);
				if (nchildren < 0)
					throw new AssertionError();
				if (nchildren > Integer.MAX_VALUE)
					throw new UnsupportedOperationException();
				buf.putInt((int) nchildren);
				sum += nchildren;
			}
			if (sum != nentries)
				throw new RuntimeException("spannedTupleCount=" + nentries
						+ ", but sum over children=" + sum);
		} else {
			/*
			 * The min is written out as a full length long value. The per child
			 * entry counts are written out using the minimum #of bits required
			 * to code the data.
			 * 
			 * Note: If min==max then ZERO bits are used per child!
			 * 
			 * The encoding takes:
			 * 
			 * nbits := 1 byte
			 * min   := 8 bytes
			 * array := BytesUtil.bitFlagByteLength((nkeys + 1)* nbits)
			 * 
			 * The encoding is byte aligned so the next data will begin on an
			 * even byte boundary.
			 */
			long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
			long sum = 0;
			for (int i = 0; i <= nkeys; i++) {
				final long nchildren = node.getChildEntryCount(i);
				sum += nchildren;
				if (nchildren < 0) {
					/*
					 * Note: ZERO is permitted for a test case, but is not legal
					 * in live data.
					 */
					throw new RuntimeException();
				}
				if (min > nchildren)
					min = nchildren;
				if (max < nchildren)
					max = nchildren;
			}
			if (sum != nentries)
				throw new RuntimeException("spannedTupleCount=" + nentries
						+ ", but sum over children=" + sum);
			if (sum == 0)
				min = max = 0;
			
            final long delta = max - min;
            assert delta >= 0;

            // will be in [1:64]
			final byte nbits = (byte) (Fast.mostSignificantBit(delta) + 1);

            // one byte.
            buf.putByte((byte) nbits);

            // offset of minVersionTimestamp.
//            O_versionTimestamps = buf.pos();

            // int64
            buf.putLong(min);

//            // int64
//            buf.putLong(max);

            if (nbits > 0) {
                /*
                 * Note: We only write the deltas if 
                 * (min!=max). When min==max, the
                 * deltas are coded in zero bits, so this would be a NOP anyway.
                 */
				final int byteLength = BytesUtil.bitFlagByteLength((nkeys + 1)
						* nbits/* nbits */);
                final byte[] a = new byte[byteLength];
                final OutputBitStream obs = new OutputBitStream(a);
                try {

                    // array of [deltaBits] length fields.
					for (int i = 0; i <= nkeys; i++) {

						final long d = node.getChildEntryCount(i) - min;

						assert d >= 0;

						obs.writeLong(d, nbits);

					}

                    obs.flush();

                    // copy onto the buffer.
                    buf.put(a);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                    // Note: close is not necessary if flushed and backed by
                    // byte[].
                    // } finally {
                    // try {
                    // obs.close();
                    // } catch (IOException e) {
                    // log.error(e);
                    // }
                }
            }

        }
        
        if(hasVersionTimestamps) {
            
            buf.putLong(node.getMinimumVersionTimestamp());

            buf.putLong(node.getMaximumVersionTimestamp());
            
        }

        // Slice onto the coded data record.
        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

        // Read-only coded IDataRecord. 
        return new ReadOnlyNodeData(slice, encodedKeys);
        
    }

    @Override
    public AbstractFixedByteArrayBuffer encode(final INodeData node,
            final DataOutputBuffer buf) {

        return encodeLive(node, buf).data();

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

        /** The record serialization version. */
        private final short version;
        
        // fields which are cached by the ctor.
        private final short flags;
        private final int nkeys;
        private final long nentries;

        /**
         * Offset of the encoded keys in the buffer.
         */
        private final int O_keys;

        /**
         * The coded keys.
         */
        private final ICodedRaba keys;

        /**
         * Offset of the encoded childAddr[] in the buffer.
         */
        private final int O_childAddr;

		/**
		 * Offset of the encoded childEntryCount[] in the buffer.
		 * 
		 * TODO Compute at runtime to save space as
		 * <code>O_childAddr + (nkeys + 1) * SIZEOF_ADDR</code>?
		 */
        private final int O_childEntryCount;
        
        /** The #of bits in the delta encoding of the childEntryCount[]. */
        private final byte childEntryCountBits;
        
        /** The minimum across the childEntryCount[]. */
        private final long minChildEntryCount;
        
        final public AbstractFixedByteArrayBuffer data() {
            
            return b;
            
        }

        /**
         * Constructor used when the caller is encoding the {@link INodeData}.
         * 
         * @param buf
         *            The buffer containing the data for the node.
         * @param keys The coded keys.
         */
        public ReadOnlyNodeData(final AbstractFixedByteArrayBuffer buf,
                final ICodedRaba keys) {

            if (buf == null)
                throw new IllegalArgumentException();

            if (keys == null)
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

            version = buf.getShort(pos);
            pos += SIZEOF_VERSION;
            switch (version) {
            case VERSION0:
            case VERSION1:
                break;
            default:
                throw new AssertionError("version=" + version);
            }
            
            // flags
            flags = buf.getShort(pos);
            pos += SIZEOF_FLAGS;

            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;

			if (version == ReadOnlyNodeData.VERSION0) {
				this.nentries = buf.getInt(pos);
				pos += Bytes.SIZEOF_INT;
			} else {
				this.nentries = buf.getLong(pos);
				pos += Bytes.SIZEOF_LONG;
			}
			if (nentries < 0) {
				/*
				 * Note: ZERO (0) is permitted for a test case but is not legal
				 * in live data.
				 */
				throw new RuntimeException();
			}
            
            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

//          O_keys = O_childEntryCount + (nkeys + 1) * SIZEOF_ENTRY_COUNT;
            O_keys = pos;
            this.keys = keys;//keysCoder.decode(buf.slice(O_keys, keysSize));
            pos += keysSize;
//            assert b.position() == O_keys + keysSize;
			if (keys.size() != nkeys) // sanity check.
				throw new RuntimeException("nkeys=" + nkeys + ", keys.size="
						+ keys.size());
            
            O_childAddr = pos;

            O_childEntryCount = O_childAddr + (nkeys + 1) * SIZEOF_ADDR;
            
			if (version >= ReadOnlyNodeData.VERSION1) {
				childEntryCountBits = buf.getByte(O_childEntryCount);
			} else {
				// Not used in this version.
				childEntryCountBits = -1;
			}
			
			minChildEntryCount = buf.getLong(O_childEntryCount + 1);

            // save reference to buffer
            this.b = buf;

        }

        /**
         * Decode in place (wraps a buffer containing an encoded node data record).
         * 
         * @param buf
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

            version = buf.getShort(pos);
            pos += SIZEOF_VERSION;
            switch (version) {
			case VERSION0:
			case VERSION1:
				break;
            default:
                throw new AssertionError("version=" + version);
            }
            
            // flags
            flags = buf.getShort(pos);
            pos += SIZEOF_FLAGS;

            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;

			if (version == ReadOnlyNodeData.VERSION0) {
				this.nentries = buf.getInt(pos);
				pos += Bytes.SIZEOF_INT;
			} else {
				this.nentries = buf.getLong(pos);
				pos += Bytes.SIZEOF_LONG;
			}
			if (nentries < 0) {
				/*
				 * Note: ZERO (0) is allowed for a unit test, but it is not
				 * legal in live data.
				 */
				throw new RuntimeException();
			}

            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

//          O_keys = O_childEntryCount + (nkeys + 1) * SIZEOF_ENTRY_COUNT;
            O_keys = pos;
            this.keys = keysCoder.decode(buf.slice(O_keys, keysSize));
            pos += keysSize;
//            assert b.position() == O_keys + keysSize;
			if (keys.size() != nkeys) // sanity check.
				throw new RuntimeException("nkeys=" + nkeys + ", keys.size="
						+ keys.size());
            
            O_childAddr = pos;
            
            O_childEntryCount = O_childAddr + (nkeys + 1) * SIZEOF_ADDR;

			if (version >= ReadOnlyNodeData.VERSION1) {
				childEntryCountBits = buf.getByte(O_childEntryCount);
			} else {
				// Not used in this version.
				childEntryCountBits = -1;
			}

			minChildEntryCount = buf.getLong(O_childEntryCount + 1);
			
			// save reference to buffer
            this.b = buf;

        }

        /**
         * The offset into the buffer of the minimum version timestamp, which is
         * an int64 field. The maximum version timestamp is the next field. This
         * offset is computed dynamically to keep down the size of the node
         * object in memory.
         */
        private int getVersionTimestampOffset() {

			if (version == ReadOnlyNodeData.VERSION0) {

				return O_childEntryCount + ((nkeys + 1) * Bytes.SIZEOF_INT);
            
        	} else {
        		/* Compute the offset to the version timestamps based on the
        		 * #of bits required to encode each entry in the child entry
        		 * count array.
        		 * 
        		 * nbits := 1 byte
        		 * min   := 8 bytes (Long)
        		 * array := BytesUtil.bitFlagByteLength((nkeys + 1)* nbits)
        		 */
				return O_childEntryCount//
						+ 1 // one byte whose value is [nbits]
						+ Bytes.SIZEOF_LONG // min
						+ BytesUtil.bitFlagByteLength((nkeys + 1)
								* childEntryCountBits);
        		
        	}
            
        }
        
        @Override
        final public boolean hasVersionTimestamps() {
            
            return ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
            
        }

        @Override
        final public long getMinimumVersionTimestamp() {
            
            if(!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            final int off = getVersionTimestampOffset();
            
            // at the offset.
            return b.getLong(off);
            
        }

        @Override
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
        @Override
        final public boolean isLeaf() {

            return false;

        }

        /**
         * Yes.
         */
        @Override
        final public boolean isReadOnly() {
            
            return true;
            
        }
        
        /**
         * Yes.
         */
        @Override
        final public boolean isCoded() {
            
            return true;
            
        }
        
        /**
         * {@inheritDoc}. This field is cached.
         */
        @Override
        final public int getKeyCount() {
            
            return nkeys;
            
        }

        /**
         * {@inheritDoc}. This field is cached.
         */
        @Override
        final public int getChildCount() {
            
            return nkeys + 1;
            
        }

        /**
         * {@inheritDoc}. This field is cached.
         */
        @Override
        final public long getSpannedTupleCount() {
            
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

        @Override
        final public long getChildAddr(final int index) {

            assert assertChildIndex(index);

            return b.getLong(O_childAddr + index * SIZEOF_ADDR);

		}

        @Override
		final public long getChildEntryCount(final int index) {

			assert assertChildIndex(index);

			if (version == ReadOnlyNodeData.VERSION0) {

				return b.getInt(O_childEntryCount + index * Bytes.SIZEOF_INT);

            } else {
                // Note: O_childEntryCount is [nbits], which is one byte.
//                final long min = b.getLong(O_childEntryCount + 1/* nbits */);

                final long bitpos = ((O_childEntryCount + 1 + Bytes.SIZEOF_LONG) << 3)
                        + ((long) index * childEntryCountBits);

//                if (childEntryCountBits <= 32) {

                final long bitIndex = (b.off() << 3) + bitpos;

                final long deltat = BytesUtil.getBits64(b.array(),
                        (int) bitIndex, childEntryCountBits);

                return minChildEntryCount + deltat;

//                }

//                final InputBitStream ibs = b.getInputBitStream();
//                try {
//
//                    ibs.position(bitpos);
//
//                    final long deltat = ibs
//                            .readLong(childEntryCountBits/* nbits */);
//
//                    return minChildEntryCount + deltat;
//                    
//                } catch(IOException ex) {
//                    
//                    throw new RuntimeException(ex);
//                    
//    // close not required for IBS backed by byte[] and has high overhead.
////                } finally {
////                    try {
////                        ibs.close();
////                    } catch (IOException ex) {
////                        log.error(ex);
////                    }
//                }
            	
            }

        }

        @Override
        final public IRaba getKeys() {

            return keys;

        }

        @Override
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

        sb.append(",\nkeys=" + node.getKeys());

        {

            sb.append(",\nchildAddr=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildAddr(i));

            }

            sb.append("]");

        }

        {

            sb.append(",\nchildEntryCount=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildEntryCount(i));

            }

            sb.append("]");

        }

        if(node.hasVersionTimestamps()) {
            
            sb.append(",\nversionTimestamps={min="
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
