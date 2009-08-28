package com.bigdata.rdf.spo;


import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.AbstractRabaDecoder;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.raba.codec.IRabaDecoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * We encode the value in 3 bits per statement. The 1st bit is the override
 * flag. The remaining two bits are the statement type {inferred, explicit, or
 * axiom}. The bit sequence <code>111</code> is used as a placeholder for a
 * <code>null</code> value and de-serializes to a [null].
 * <p>
 * Note: the 'override' flag is NOT stored in the statement indices, but it is
 * passed by the procedure that writes on the statement indices so that we can
 * decide whether or not to override the type when the statement is pre-existing
 * in the index.
 * <p>
 * Note: this procedure can not be used if
 * {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} are enabled.
 * 
 * @todo test suite (this is currently tested by its use on stores where
 *       statement identifiers are not enabled).
 * 
 * @see StatementEnum
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FastRDFValueCompression implements Externalizable, IRabaCoder {

    protected static final Logger log = Logger
            .getLogger(FastRDFValueCompression.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1933430721504168533L;

    /**
     * The only version defined so far.
     */
    private static final byte VERSION0 = 0x00;
    
    /**
     * No.
     */
    final public boolean isKeyCoder() {
        return false;
    }

    /**
     * Yes.
     */
    final public boolean isValueCoder() {
        return true;
    }

    /**
     * Sole constructor (handles de-serialization also).
     */
    public FastRDFValueCompression() {

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP

    }

    /**
     * Code the record.
     */
    protected void writeOn(final IRaba raba, final OutputBitStream obs)
            throws IOException {

        final int n = raba.size();

        assert n >= 0;

        obs.writeInt(VERSION0, 8/* nbits */);

        obs.writeNibble(n);

        for (int i = 0; i < n; i++) {

            if (raba.isNull(i)) {

                // flag a deleted value (de-serialize to a null).
                obs.writeInt( 7, 3 );
                
            } else {

                final byte[] val = raba.get(i);

                obs.writeInt((int) val[0], 3);
                
            }

        }
        
        // ALWAYS FLUSH.
        obs.flush();
        
    }
    
    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer b) {

        if (raba == null)
            throw new UnsupportedOperationException();

        if (b == null)
            throw new UnsupportedOperationException();
        
        final int n = raba.size();
        
        // This is sufficient capacity to code the data.
        final int initialCapacity = Bytes.SIZEOF_INT + (3 * n) / 8 + 1;

        b.ensureCapacity(initialCapacity);

//        final FastByteArrayOutputStream baos = new FastByteArrayOutputStream(
//                initialCapacity);

//        if (b == null) {
//
//            b = new ByteArrayBuffer(initialCapacity);
//            
//        } else {
//
//            b.ensureCapacity(initialCapacity);
//            
//            b.reset();
//            
//        }

        // The byte offset of the start of the coded record in the buffer.
        final int O_origin = b.pos();
        
        final OutputBitStream obs = new OutputBitStream(b, 0 /* unbuffered! */,
                false/* reflectionTest */);

        try {
         
            // code the data.
            writeOn(raba, obs);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        return b.slice(O_origin, b.pos() - O_origin);
//        return b.toByteArray();
//        return decode(ByteBuffer
//                .wrap(baos.array, 0/* off */, baos.length/* len */));
        
    }

    public IRabaDecoder decode(final AbstractFixedByteArrayBuffer data) {

        return new RabaDecoderImpl(data);
        
    }

    /**
     * Decoder.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class RabaDecoderImpl extends AbstractRabaDecoder {

        private final AbstractFixedByteArrayBuffer data;
        
        /**
         * Cached.
         */
        private final int size;
        
        /**
         * Bit offset to the first coded value.
         */
        private final int O_values;
        
        final public AbstractFixedByteArrayBuffer data() {

            return data;
            
        }

        /**
         * No.
         */
        final public boolean isKeys() {
         
            return false;
            
        }

        final public int size() {
            
            return size;
            
        }

        final public int capacity() {
            
            return size;
            
        }

        final public boolean isEmpty() {
            
            return size == 0;
            
        }

        /**
         * Always <code>true</code>.
         */
        final public boolean isFull() {

            return true;
            
        }

        public RabaDecoderImpl(final AbstractFixedByteArrayBuffer data) {

            if (data == null)
                throw new IllegalArgumentException();

            this.data = data;

            final InputBitStream ibs = data.getInputBitStream();

            try {

                final byte version = (byte) ibs.readInt(8/* nbits */);

                if (version != VERSION0) {

                    throw new IOException("Unknown version=" + version);

                }

                size = ibs.readNibble();

                O_values = (int) ibs.readBits();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            } finally {
                
                try {

                    ibs.close();

                } catch (IOException ex) {

                    log.error(ex);
                    
                }
                
            }

        }

        /**
         * Thread-safe extract of the bits coded value for the specified index.
         * 
         * @param index
         *            The specified index.
         * 
         * @return The bit coded value.
         * 
         * @throws IndexOutOfBoundsException
         *             unless the index is in [0:size-1].
         * 
         * @todo this could be faster if we extracted a byte or two and did the
         *       appropriate bit manipulations.
         */
        final protected byte getBits(final int index) {

            if (index <= 0 || index > size)
                throw new IndexOutOfBoundsException();

            int value = 0;
            long bitIndex = O_values;
            for (int i = 0; i < 3; i++, bitIndex++) {

                /*
                 * FIXME This is going to be broken due to the changes to
                 * BytesUtil to use the same big endian format for bit flags as
                 * OutputBitStream.
                 */
                final boolean bit = data.getBit(bitIndex);

                value |= (bit ? 1 : 0) << i;

            }

            return (byte) (value & 0xff);

        }
        
        final public int copy(final int index, final OutputStream os) {

            final byte bits = getBits(index);

            if (bits == 7) {

                // A null.
                throw new NullPointerException();

            } else {

                try {
                    
                    os.write(bits);
            
                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }

            }
            
            return 1;
            
        }

        final public byte[] get(final int index) {

            final byte bits = getBits(index);

            if (bits == 7) {

                // A null.
                return null;

            } else {

                return new byte[] { bits };

            }

        }

        final public boolean isNull(final int index) {

            return getBits(index) == 7;
            
        }

        /**
         * Returns ONE (1) unless the value is a <code>null</code>.
         * 
         * {@inheritDoc}
         */
        final public int length(final int index) {
            
            if (isNull(index))
                throw new NullPointerException();

            return 1;

        }

        /**
         * Not supported.
         */
        final public int search(final byte[] searchKey) {

            throw new UnsupportedOperationException();
            
        }
        
    }
    
}
