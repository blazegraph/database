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

package com.bigdata.htree.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.data.AbstractReadOnlyNodeData;
import com.bigdata.btree.data.IAbstractNodeDataCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.util.Bytes;

/**
 * Default implementation for immutable {@link IDirectoryData} records.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DefaultNodeCoder.java 4585 2011-06-01 13:42:56Z thompsonbry $
 * 
 * TODO Support RWStore 4 byte native addrs.
 * 
 * TODO Explicit test suite based on the B+Tree test suites.
 */
public class DefaultDirectoryPageCoder implements IAbstractNodeDataCoder<IDirectoryData>,
        Externalizable {

	/**
	 * The initial version of the serialized representation of the
	 * {@link DefaultDirectoryPageCoder} class (versus the serialized
	 * representation of the node or leaf).
	 */
    private final static transient byte VERSION0 = 0x00;
    
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();
        switch(version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(VERSION0);
        
    }

    /** No. */
    final public boolean isLeafDataCoder() {
        
        return false;
        
    }

    /** Yes. */
    public boolean isNodeDataCoder() {

        return true;
        
    }

//    public String toString() {
//
//        return super.toString() + "{keysCoder=" + keysCoder + "}";
//
//    }
    
    /**
     * De-serialization ctor.
     */
    public DefaultDirectoryPageCoder() {
        
    }
    
    public IDirectoryData decode(final AbstractFixedByteArrayBuffer data) {

        return new ReadOnlyDirectoryData(data);
        
    }

    public IDirectoryData encodeLive(final IDirectoryData node, final DataOutputBuffer buf) {

        if (node == null)
            throw new IllegalArgumentException();

        if (buf == null)
            throw new IllegalArgumentException();

        final short version = ReadOnlyDirectoryData.currentVersion;
        
        // The byte offset of the start of the coded data in the buffer.
        final int O_origin = buf.pos();
        
        buf.putByte(ReadOnlyDirectoryData.NODE);

        buf.putShort(version);

		final boolean hasVersionTimestamps = node.hasVersionTimestamps();
		short flags = 0;
		if (hasVersionTimestamps) {
			flags |= AbstractReadOnlyNodeData.FLAG_VERSION_TIMESTAMPS;
			throw new UnsupportedOperationException();
		}
		if(node.isOverflowDirectory()) {
		    flags |= AbstractReadOnlyNodeData.FLAG_OVERFLOW_DIRECTORY;
		}
        buf.putShort(flags);
        
        if (node.isOverflowDirectory()) {
            final byte[] ok = node.getOverflowKey();
        	buf.putShort((short) ok.length);
        	buf.put(ok);
        }
        
		final int nchildren = node.getChildCount();
		if (nchildren > Short.MAX_VALUE)
			throw new UnsupportedOperationException();
		buf.putShort((short) nchildren);

        // childAddr[] : TODO code childAddr[] (if RWSTORE bit is set, covert to 4 byte native addr).
        for (int i = 0; i < nchildren; i++) {
            buf.putLong(node.getChildAddr(i));
        }
        
        // Slice onto the coded data record.
        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

        // Read-only coded IDataRecord. 
        return new ReadOnlyDirectoryData(slice);
        
    }

    public AbstractFixedByteArrayBuffer encode(final IDirectoryData node,
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
     * @version $Id: DefaultNodeCoder.java 4585 2011-06-01 13:42:56Z thompsonbry $
     */
    static private class ReadOnlyDirectoryData extends AbstractReadOnlyNodeData<IDirectoryData>
            implements IDirectoryData {
        
        /** The backing buffer */
        private final AbstractFixedByteArrayBuffer b;

        /** The record serialization version. */
        private final short version;
        
        // fields which are cached by the ctor.
        private final short flags;
        private final int nchildren;
        
        private final byte[] overflowKey;

        /**
         * Offset of the encoded childAddr[] in the buffer.
         */
        private final int O_childAddr;
        
        final public AbstractFixedByteArrayBuffer data() {
            
            return b;
            
        }
        
        //static int s_newKeys = 0;

        /**
         * Constructor used when the caller is encoding the {@link IDirectoryData}.
         * 
         * @param buf
         *            The buffer containing the data for the node.
         * @param keys The coded keys.
         */
        public ReadOnlyDirectoryData(final AbstractFixedByteArrayBuffer buf) {

            if (buf == null)
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
            
    		if (isOverflowDirectory()) {
                final int oksze = buf.getShort(pos);
                pos += Bytes.SIZEOF_SHORT;
            	overflowKey = new byte[oksze];
            	//s_newKeys++;
            	
            	buf.get(pos, overflowKey);           	
            	pos += oksze;
    		} else {
            	overflowKey = null;
    		}


            nchildren = buf.getShort(pos);
            pos += Bytes.SIZEOF_SHORT;
            
            O_childAddr = pos;

            // save reference to buffer
            this.b = buf;

        }
        
        final public boolean hasVersionTimestamps() {
            
            return ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
            
        }

        final public long getMinimumVersionTimestamp() {
            
            if(!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            // Note: version timestamps not yet supported by HTree.
            throw new UnsupportedOperationException();
            
        }

        final public long getMaximumVersionTimestamp() {
            
            if(!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            // Note: version timestamps not yet supported by HTree.
            throw new UnsupportedOperationException();
            
        }

        final public boolean isOverflowDirectory() {
            
            return (flags & AbstractReadOnlyNodeData.FLAG_OVERFLOW_DIRECTORY) != 0;
                        
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
        
		final public int getChildCount() {

			return nchildren;

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
            
            if (index < 0 || index > nchildren)
                throw new IndexOutOfBoundsException("index=" + index
                        + ", nchildren=" + nchildren);

            return true;
            
        }

        final public long getChildAddr(final int index) {

            assert assertChildIndex(index);

            return b.getLong(O_childAddr + index * SIZEOF_ADDR);

		}

        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getName() + "{");

            DefaultDirectoryPageCoder.toString(this, sb);

            sb.append("}");
            
            return sb.toString();
            
        }

		public byte[] getOverflowKey() {
			return overflowKey;
		}

    }

    /**
     * Utility method formats the {@link IDirectoryData}.
     * 
     * @param node
     *            A node data record.
     * @param sb
     *            The representation will be written onto this object.
     * 
     * @return The <i>sb</i> parameter.
     */
    static public StringBuilder toString(final IDirectoryData node,
            final StringBuilder sb) {

        final int nchildren = node.getChildCount();

        sb.append(", nchildren=" + nchildren);

        {

            sb.append(",\nchildAddr=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(node.getChildAddr(i));

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
