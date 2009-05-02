package com.bigdata.btree.compression;

import it.unimi.dsi.fastutil.bytes.CustomByteArrayFrontCodedList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * Prefix compression.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PrefixSerializer implements IDataSerializer, Externalizable {
    
    protected static final Logger log = Logger.getLogger(PrefixSerializer.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -5838814203192637411L;
    
    public final transient static PrefixSerializer INSTANCE = new PrefixSerializer();

    public PrefixSerializer() {
        
    }
    
    /**
     * The #of keys is written on the output stream. If there is only a single
     * key, the we just write out the #of bytes in the key followed by the key
     * itself. If there is more than one key, then the front-coded byte array is
     * computed. The front coded byte array is serialized as the #of bytes in
     * the front-coded array followed by the front-coded array. The ratio for
     * the front-coded array is always the #of keys in the array since we want
     * as much compression as possible and since access during decompression is
     * serial.
     */
    public void write(final DataOutput out, final IRandomAccessByteArray raba)
            throws IOException {

        /*
         * The ratio as defined by {@link ByteArrayFrontCodedList}. If you
         * are only using serial access for the data, then this should be
         * the maximum capacity of the array, e.g., the branching factor of
         * the B+Tree, as that will give the best compression.
         */
        final int n = raba.getKeyCount();
        final int ratio = n;

        if (INFO)
            log.info("ratio=" + ratio + ", n=" + raba.getKeyCount()
                    + ", capacity=" + raba.getMaxKeys());

        // note: zero length indicates NO data.
        out.writeInt(n);

        if (n == 0) {
            
            // no keys.
            
            return;

        }

        if(n == 1) {
            
            // one key.
            
            final byte[] key = raba.getKey(0);
            
            out.writeInt(key.length);
            
            out.write(key);
            
            return;
            
        }

        // more than one key.
        
        final byte[] data;
        {
            final Iterator<byte[]> itr = raba.iterator();

            final CustomByteArrayFrontCodedList c = new CustomByteArrayFrontCodedList(
                    itr, ratio);

            data = c.getArray();
            // final byte[] data = SerializerUtil.serialize(c);
        
        }

        // #of bytes.
        out.writeInt(data.length);

        // serialized front-coded array.
        out.write(data);

    }

    public void read(final DataInput in, final IRandomAccessByteArray raba)
            throws IOException {

        final int nkeys = in.readInt();

        if (nkeys == 0) {

            /*
             * No keys.
             */

            return;

        }

        if (nkeys == 1) {

            /*
             * One key.
             */

            // #of bytes in the key.
            final int nbytes = in.readInt();

            // allocate the array.
            final byte[] key = new byte[nbytes];

            // read the front coded array.
            in.readFully(key);

            raba.add(key);
            
            return;
            
        }

        /*
         * More than one key.
         */
        
        // #of bytes in the front coded array.
        final int nbytes = in.readInt();
        
        // allocate the array.
        final byte[] data = new byte[nbytes];
        
        // read the front coded array.
        in.readFully(data);
        
        final int ratio = nkeys;
        
        final CustomByteArrayFrontCodedList c = new CustomByteArrayFrontCodedList(
                nkeys, ratio, data);
        
//        final CustomByteArrayFrontCodedList c;
//        try {
//
//            c = (CustomByteArrayFrontCodedList) new ObjectInputStream(
//                    new ByteArrayInputStream(data)).readObject();
//            
//        } catch (ClassNotFoundException e) {
//            
//            throw new IOException(e.getLocalizedMessage());
//            
//        }
        
        final Iterator<byte[]> itr = c.iterator();
        
        while(itr.hasNext()) {
            
            raba.add(itr.next());
            
        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP - this class has no state.

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP - this class has no state.

    }

}
