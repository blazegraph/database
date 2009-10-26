package com.bigdata.rdf.spo;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.AbstractCodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Coder for statement index with inference enabled but without SIDS. We encode
 * the value in 3 bits per statement. The 1st bit is the override flag. The
 * remaining two bits are the statement type {inferred, explicit, or axiom}. The
 * bit sequence <code>111</code> is used as a place holder for a
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
 * @see StatementEnum
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link FastRDFValueCoder2}
 */
public class FastRDFValueCoder implements Externalizable, IRabaCoder {

    protected static final Logger log = Logger
            .getLogger(FastRDFValueCoder.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1933430721504168533L;

    /**
     * The only version defined so far.
     */
    private static transient final byte VERSION0 = 0x00;

    /**
     * The bit offset of the start of the coded values. Each value is 3 bits.
     */
    private static transient final long O_values = (1/* version */+ Bytes.SIZEOF_INT/* size */) << 3;
    
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
    public FastRDFValueCoder() {

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP

    }

    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        /*
         * Note: This code path has nearly zero overhead when compared to
         * encodeLive().
         */
        
        return encodeLive(raba, buf).data();

    }

    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        if (raba == null)
            throw new UnsupportedOperationException();

        if (buf == null)
            throw new UnsupportedOperationException();
        
        final int n = raba.size();
        
        // This is sufficient capacity to code the data.
        final int initialCapacity = 1 + Bytes.SIZEOF_INT
                + BytesUtil.bitFlagByteLength(3 * n);

        buf.ensureCapacity(initialCapacity);

        // The byte offset of the start of the coded record in the buffer.
        final int O_origin = buf.pos();
        
        final int size = raba.size();

        buf.putByte(VERSION0);

        buf.putInt(size);

        /*
         * @todo use variant OBS(byte[], off, len) constructor and
         * pre-extend the buffer to have sufficient capacity so the OBS can
         * write directly onto the backing byte[], which will be much
         * faster.
         */
        final OutputBitStream obs = buf.getOutputBitStream();
//        final long O_values;
        try {

//            obs.writeInt(VERSION0, 8/* nbits */);
//
//            obs.writeNibble(size);
//
//            // Note: the bit offset where we start to code the values.
//            O_values = obs.writtenBits();
            
            for (int i = 0; i < size; i++) {

                if (raba.isNull(i)) {

                    // flag a deleted value (de-serialize to a null).
                    obs.writeInt(7, 3/* nbits */);
                    
                } else {

                    final byte[] val = raba.get(i);

                    obs.writeInt((int) val[0], 3/* nbits */);
                    
                }

            }
            
            // ALWAYS FLUSH.
            obs.flush();
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        // slice on just the coded data record.
        final AbstractFixedByteArrayBuffer slice = buf.slice(O_origin, buf.pos()
                - O_origin);

//        // adjusted bit offset to the start of the coded values in the slice.
//        final int O_valuesAdjusted = ((int) O_values) - O_origin << 3;
//
//        return new CodedRabaImpl(slice, size, O_valuesAdjusted);

        return new CodedRabaImpl(slice, size);

    }

    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {

        return new CodedRabaImpl(data);
        
    }

    /**
     * Decoder.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class CodedRabaImpl extends AbstractCodedRaba {

        private final AbstractFixedByteArrayBuffer data;
        
        /**
         * Cached.
         */
        private final int size;
        
//        /**
//         * Bit offset to the first coded value.
//         */
//        private final int O_values;
        
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

        /**
         * Constructor used when encoding a data record.
         * 
         * @param data
         *            The coded data record.
         * @param size
         *            The size of the coded {@link IRaba}.
         */
//        * @param O_values
//        *            The bit offset of the start of the coded values.
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data,
                final int size) { //, final int O_values) {

            this.data = data;

            this.size = size;
            
//            this.O_values = O_values;
            
        }

        /**
         * Constructor used when decoding a data record.
         * 
         * @param data
         *            The coded data record.
         */
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data) {

            if (data == null)
                throw new IllegalArgumentException();

            this.data = data;

            final byte version = data.getByte(0/*off*/);
            
            if (version != VERSION0) {

                throw new RuntimeException("Unknown version=" + version);

            }

            size = data.getInt(1/*off*/);//ibs.readNibble();
            
//            final InputBitStream ibs = data.getInputBitStream();
//
//            try {
//
//                final byte version = (byte) ibs.readInt(8/* nbits */);
//
//                if (version != VERSION0) {
//
//                    throw new RuntimeException("Unknown version=" + version);
//
//                }
//
//                size = ibs.readNibble();
//
//                O_values = (int) ibs.readBits();
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//// close not required for IBS backed by byte[] and has high overhead.
////            } finally {
////                
////                try {
////
////                    ibs.close();
////
////                } catch (IOException ex) {
////
////                    log.error(ex);
////                    
////                }
////                
//            }

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
         */
        final protected byte getBits(final int index) {

            if (index < 0 || index >= size)
                throw new IndexOutOfBoundsException();

            final InputBitStream ibs = data.getInputBitStream();
            try {

                ibs.position(O_values + (index * 3L));

                final int value = ibs.readInt(3/* nbits */);

                return (byte) (0xff & value);
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
// close not required for IBS backed by byte[] and has high overhead.
//            } finally {
//                try {
//                ibs.close();
//                } catch(IOException ex) {
//                    log.error(ex);
//                }
            }
            
//            int value = 0;
//            
//            for (int i = 0; i < 3; i++, bitIndex++) {
//
//                final boolean bit = data.getBit(bitIndex);
//
//                value |= (bit ? 1 : 0) << i;
//
//            }
//
//            return (byte) (value & 0xff);

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
