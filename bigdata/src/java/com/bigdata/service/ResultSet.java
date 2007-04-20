package com.bigdata.service;

import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;

/**
 * An object used to stream key scan results back to the client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResultSet implements Externalizable {
    
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
    public int getRangeCount() {return rangeCount;}
    
    /**
     * Actual #of key-value pairs in the {@link ResultSet}
     */
    public int getNumTuples() {return ntuples;}

    /**
     * True iff the iterator exhausted the available keys such that no more
     * results would be available if you formed the successor of the
     * {@link #lastKey}.
     */
    public boolean isExhausted() {return exhausted;}
        
    /**
     * The last key visited by the iterator <em>regardless</em> of the
     * filter imposed -or- <code>null</code> iff no keys were visited by
     * the iterator for the specified key range.
     * 
     * @see #successor()
     */
    public byte[] getLastKey() {return lastKey;}

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
    public byte[][] getKeys() {return keys;}

    /**
     * The visited values iff the values were requested.
     */
    public byte[][] getValues() {return vals;}
    
    /**
     * Deserialization constructor.
     */
    public ResultSet() {}

    /**
     * Constructor used by the {@link DataService} to populate the
     * {@link ResultSet}.
     * 
     * @param ndx
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param sendKeys
     * @param sendVals
     */
    public ResultSet(final IIndex ndx, final byte[] fromKey,
            final byte[] toKey, final int capacity, final boolean sendKeys,
            final boolean sendVals) {

        // The upper bound on the #of key-value pairs in the range.
        rangeCount = ndx.rangeCount(fromKey, toKey);

        final int limit = (rangeCount > capacity ? capacity : rangeCount);

        int ntuples = 0;

        keys = (sendKeys ? new byte[limit][] : null);

        vals = (sendVals ? new byte[limit][] : null);

        // iterator that will visit the key range.
        IEntryIterator itr = ndx.rangeIterator(fromKey, toKey);

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
        
        this.exhausted = ! itr.hasNext();
        
    }

    protected static short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        final short version = ShortPacker.unpackShort(in);
        
        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);
        
        rangeCount = (int) LongPacker.unpackLong(in);
        ntuples = (int)LongPacker.unpackLong(in);
        exhausted = in.readBoolean();
        final boolean haveKeys = in.readBoolean();
        final boolean haveVals = in.readBoolean();
        final int lastKeySize = (int)LongPacker.unpackLong(in);
        if(lastKeySize!=0) {
            lastKey = new byte[lastKeySize];
            in.readFully(lastKey);
        } else {
            lastKey = null;
        }
        if(haveKeys) {
            keys = new byte[ntuples][];
            for(int i=0;i<ntuples;i++) {
                int size = (int)LongPacker.unpackLong(in);
                byte[] tmp = new byte[size];
                in.readFully(tmp);
                keys[i] = tmp;
            }
        } else {
            keys = null;
        }
        if(haveVals) {
            vals = new byte[ntuples][];
            for(int i=0;i<ntuples;i++) {
                int size = (int)LongPacker.unpackLong(in);
                byte[] tmp = new byte[size];
                in.readFully(tmp);
                vals[i] = tmp;
            }
        } else {
            vals = null;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        /*
         * @todo once I have some benchmarks for the data service protocol,
         * try the changes commented out below and see if the performance is
         * better when I explicitly buffer the writes.
         */
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(100 + ntuples
//                * 512);
//        DataOutput dos = new DataOutputStream(baos);
        DataOutput dos = out;

        ShortPacker.packShort(dos, VERSION0);
        LongPacker.packLong(dos, rangeCount);
        LongPacker.packLong(dos,ntuples);
        dos.writeBoolean(exhausted);
        dos.writeBoolean(keys!=null);
        dos.writeBoolean(vals!=null);
        LongPacker.packLong(dos,lastKey==null?0:lastKey.length);
        if(lastKey!=null) {
            dos.write(lastKey);
        }
        if(keys!=null) {
            for(int i=0; i<ntuples; i++) {
                LongPacker.packLong(dos, keys[i].length);
                dos.write(keys[i]);
            }
        }
        if(vals!=null) {
            for(int i=0; i<ntuples; i++) {
                LongPacker.packLong(dos, vals[i].length);
                dos.write(vals[i]);
            }
        }
        
//        out.write(baos.toByteArray());
    }

}
