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
package com.bigdata.btree;

import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;
import org.apache.log4j.Logger;

import com.bigdata.btree.IDataSerializer.DefaultDataSerializer;
import com.bigdata.service.DataService;

/**
 * An object used to stream key scan results back to the client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResultSet implements Externalizable {

    protected static final Logger log = Logger.getLogger(ResultSet.class);

    /**
     * 
     */
    private static final long serialVersionUID = 8823205844134046434L;

    private int rangeCount;

    private int ntuples;

    private boolean exhausted;

    private byte[] lastKey;

    private byte[][] keys;

    private byte[][] vals;

    /**
     * Total #of key-value pairs within the key range (approximate).
     */
    public int getRangeCount() {
        return rangeCount;
    }

    /**
     * Actual #of key-value pairs in the {@link ResultSet}
     */
    public int getNumTuples() {
        return ntuples;
    }

    /**
     * True iff the iterator exhausted the available keys such that no more
     * results would be available if you formed the successor of the
     * {@link #lastKey}.
     */
    public boolean isExhausted() {
        return exhausted;
    }

    /**
     * The last key visited by the iterator <em>regardless</em> of the
     * filter imposed -or- <code>null</code> iff no keys were visited by
     * the iterator for the specified key range.
     * 
     * @see #successor()
     */
    public byte[] getLastKey() {
        return lastKey;
    }

    /**
     * The next key that should be used to retrieve keys and/or values starting
     * from the first possible successor of the {@link #getLastKey()} visited by
     * the iterator in this operation (the successor is formed by appending a
     * <code>nul</code> byte to the {@link #getLastKey()}).
     * 
     * @return The successor of {@link #getLastKey()} -or- <code>null</code>
     *         iff the iterator exhausted the available keys.
     * 
     * @exception UnsupportedOperationException
     *                if the {@link #lastKey} is <code>null</code>.
     */
    public byte[] successor() {

        if (lastKey == null)
            throw new UnsupportedOperationException();

        return BytesUtil.successor(lastKey);

    }

    /**
     * The visited keys iff the keys were requested.
     */
    public byte[][] getKeys() {
        return keys;
    }

    /**
     * The visited values iff the values were requested.
     */
    public byte[][] getValues() {
        return vals;
    }

    /**
     * Deserialization constructor.
     */
    public ResultSet() {
    }

    /**
     * Return the object used to (de-)serialize the keys.
     * <p>
     * Note: This returns the {@link DefaultDataSerializer} by default and MAY
     * be overriden to use custom serialization or compression of the keys.
     */
    protected IDataSerializer getKeySerializer() {

        return new DefaultDataSerializer();

    }

    /**
     * Return the object used to (de-)serialize the values.
     * <p>
     * Note: This returns the {@link DefaultDataSerializer} by default and MAY
     * be overriden to use custom serialization or compression of the values.
     */
    protected IDataSerializer getValSerializer() {

        return new DefaultDataSerializer();

    }

//    /**
//     * Interface used to construct instances of {@link ResultSet}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static interface IResultSetConstructor extends Serializable {
//
//        public ResultSet newResultSet(IIndex ndx, byte[] fromKey, byte[] toKey,
//                int capacity, int flags, IEntryFilter filter);
//
//    }
//
//    /**
//     * Default implementation.
//     * <p>
//     * Note: Derived implementations can return a subclass of {@link ResultSet}
//     * that uses custom serialization for the keys and/or values.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class DefaultResultSetConstructor implements
//            IResultSetConstructor {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = -237842375057508872L;
//
//        public static transient IResultSetConstructor INSTANCE = new DefaultResultSetConstructor();
//        
//        public ResultSet newResultSet(IIndex ndx, byte[] fromKey, byte[] toKey,
//                int capacity, int flags, IEntryFilter filter) {
//            
//            return new ResultSet(ndx, fromKey, toKey, capacity, flags, filter);
//            
//        }
//        
//    }
    
    /**
     * Constructor used by the {@link DataService} to populate the
     * {@link ResultSet}.
     * 
     * @param ndx
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
    public ResultSet(IIndex ndx, byte[] fromKey, byte[] toKey, int capacity,
            int flags, IEntryFilter filter) {

        /*
         * The upper bound on the #of key-value pairs in the range.
         * 
         * Note: truncated to [int].
         */
        rangeCount = (int) ndx.rangeCount(fromKey, toKey);

        final int limit = (rangeCount > capacity ? capacity : rangeCount);

        int ntuples = 0;

        final boolean sendKeys = (flags & IRangeQuery.KEYS) != 0;

        final boolean sendVals = (flags & IRangeQuery.VALS) != 0;

        keys = (sendKeys ? new byte[limit][] : null);

        vals = (sendVals ? new byte[limit][] : null);

        /*
         * Iterator that will visit the key range.
         * 
         * Note: We always visit the keys regardless of whether we pass them on
         * to the caller. This is necessary in order for us to set the [lastKey]
         * field on the result set.
         */
        final IEntryIterator itr = ndx.rangeIterator(fromKey, toKey, capacity,
                flags | IRangeQuery.KEYS, filter);

        /*
         * true if any keys were visited regardless of whether or not they
         * satisified the optional filter. This is used to make sure that we
         * always return the lastKey visited if any keys were visited and
         * otherwise set lastKey := null.
         */
        boolean anything = false;

        while (ntuples < limit && itr.hasNext()) {

            anything = true;

            byte[] val = (byte[]) itr.next();

            if (sendVals)
                vals[ntuples] = val;

            if (sendKeys)
                keys[ntuples] = itr.getKey();

            // #of results that will be returned.
            ntuples++;

        }

        this.ntuples = ntuples;

        this.lastKey = (anything ? itr.getKey() : null);

        this.exhausted = !itr.hasNext();

        log.info("ntuples=" + ntuples + ", capacity=" + capacity
                + ", exhausted=" + exhausted + ", sendKeys=" + sendKeys
                + ", sendVals=" + sendVals);

    }

    protected static short VERSION0 = 0x0;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = ShortPacker.unpackShort(in);

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        rangeCount = (int) LongPacker.unpackLong(in);

        ntuples = (int) LongPacker.unpackLong(in);

        exhausted = in.readBoolean();

        final boolean haveKeys = in.readBoolean();

        final boolean haveVals = in.readBoolean();

        final int lastKeySize = (int) LongPacker.unpackLong(in);
        if (lastKeySize != 0) {
            lastKey = new byte[lastKeySize];
            in.readFully(lastKey);
        } else {
            lastKey = null;
        }

        if (haveKeys) {
            keys = getKeySerializer().read(in);
        } else {
            keys = null;
        }

        if (haveVals) {
            vals = getValSerializer().read(in);
        } else {
            vals = null;
        }

        //            if (haveKeys) {
        //                keys = new byte[ntuples][];
        //                for (int i = 0; i < ntuples; i++) {
        //                    int size = (int) LongPacker.unpackLong(in);
        //                    byte[] tmp = new byte[size];
        //                    in.readFully(tmp);
        //                    keys[i] = tmp;
        //                }
        //            } else {
        //                keys = null;
        //            }

        //            if (haveVals) {
        //                vals = new byte[ntuples][];
        //                for (int i = 0; i < ntuples; i++) {
        //                    // when lenPlus == 0 the value is null (vs byte[0]).
        //                    final int lenPlus1 = (int) LongPacker.unpackLong(in);
        //                    if (lenPlus1 > 0) {
        //                        byte[] tmp = new byte[lenPlus1 - 1];
        //                        in.readFully(tmp);
        //                        vals[i] = tmp;
        //                    } else
        //                        vals[i] = null;
        //                }
        //            } else {
        //                vals = null;
        //            }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        DataOutput dos = out;

        ShortPacker.packShort(dos, VERSION0);

        LongPacker.packLong(dos, rangeCount);

        LongPacker.packLong(dos, ntuples);

        dos.writeBoolean(exhausted);

        dos.writeBoolean(keys != null);

        dos.writeBoolean(vals != null);

        LongPacker.packLong(dos, lastKey == null ? 0 : lastKey.length);

        if (lastKey != null) {

            dos.write(lastKey);

        }

        if (keys != null) {

            getKeySerializer().write(ntuples, 0/*offset*/, keys, out);

        }

        if (vals != null) {

            getValSerializer().write(ntuples, 0/*offset*/, vals, out);

        }

        //            if (keys != null) {
        //                for (int i = 0; i < ntuples; i++) {
        //                    // keys are never null.
        //                    LongPacker.packLong(dos, keys[i].length);
        //                    dos.write(keys[i]);
        //                }
        //            }
        //            if (vals != null) {
        //                for (int i = 0; i < ntuples; i++) {
        //                    final byte[] val = vals[i];
        //                    // this differentiates a null value from an empty byte[].
        //                    final int lenPlus1 = val == null ? 0 : val.length + 1;
        //                    LongPacker.packLong(dos, lenPlus1);
        //                    if (val != null) {
        //                        dos.write(val);
        //                    }
        //                }
        //            }

    }

}
