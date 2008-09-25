package com.bigdata.btree.compression;

import it.unimi.dsi.fastutil.bytes.ByteArrayFrontCodedList;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.io.SerializerUtil;

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
    
    public void write(DataOutput out, IRandomAccessByteArray raba) throws IOException {

        /*
         * The ratio as defined by {@link ByteArrayFrontCodedList}. If you
         * are only using serial access for the data, then this should be
         * the maximum capacity of the array, e.g., the branching factor of
         * the B+Tree, as that will give the best compression.
         */
        final int ratio = raba.getKeyCount();

        if (ratio == 0) {
            
            // note: zero length indicates NO data.
            out.writeInt(0);
            
            return;
            
        }
        
        final Iterator<byte[]> itr = raba.iterator();
        
        if (INFO)
            log.info("ratio=" + ratio + ", n=" + raba.getKeyCount()
                    + ", capacity=" + raba.getMaxKeys());

        final ByteArrayFrontCodedList c = new ByteArrayFrontCodedList(itr,
                ratio);

        final byte[] data = SerializerUtil.serialize(c);

        // #of bytes.
        out.writeInt(data.length);

        // serialized front-coded array.
        out.write(data);

    }

    public void read(DataInput in, IRandomAccessByteArray raba)
            throws IOException {

        final int nbytes = in.readInt();

        if (nbytes == 0) {

            // Note: No data in the array.
            return;
            
        }
        
        final byte[] data = new byte[nbytes];
        
        in.readFully(data);
        
        final ByteArrayFrontCodedList c;
        try {

            c = (ByteArrayFrontCodedList) new ObjectInputStream(
                    new ByteArrayInputStream(data)).readObject();
            
        } catch (ClassNotFoundException e) {
            
            throw new IOException(e.getLocalizedMessage());
            
        }
        
        final Iterator<byte[]> itr = c.iterator();
        
        while(itr.hasNext()) {
            
            raba.add(itr.next());
            
        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        // NOP - this class has no state.
        
    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        // NOP - this class has no state.
        
    }

}
