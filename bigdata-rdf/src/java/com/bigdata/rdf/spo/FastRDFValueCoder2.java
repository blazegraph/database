package com.bigdata.rdf.spo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.AbstractCodedRaba;
import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Coder for values in statement index with inference enabled but without SIDS.
 * We encode the value in 4 bits per statement. The 1st bit is the override
 * flag. The remaining next two bits are the statement type {inferred, explicit,
 * or axiom}. The last bit is not used. The bit sequence <code>0111</code> is
 * used as a place holder for a <code>null</code> value and de-serializes to a
 * [null]. This is just the low nibble of the {@link StatementEnum#code()}. This
 * "nibble" encoding makes it fast and easy to extract the value from the coded
 * record. The first value is stored in the low nibble, the next in the high
 * nibble, then it is on to the low nibble of the next byte.
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
 * @todo A mutable coded value raba could be implemented for the statement
 *       indices. With a fixed bit length per value, we can represent the data
 *       in m/2 bytes. This is also true for things like TERM2ID where the
 *       values could be represented as a long[].
 */
public class FastRDFValueCoder2 implements Externalizable, IRabaCoder {

    protected static final Logger log = Logger
            .getLogger(FastRDFValueCoder2.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1933430721504168533L;

    /**
     * The only version defined so far.
     */
    private static transient final byte VERSION0 = 0x00;

    /**
     * The byte offset of the start of the coded values. Each value is 4 bits.
     */
    private static transient final int O_values = (1/* version */+ Bytes.SIZEOF_INT/* size */);
    
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
    public FastRDFValueCoder2() {

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
        
        // This is the exact capacity required to code the data.
        final int initialCapacity = 1 + Bytes.SIZEOF_INT
                + BytesUtil.bitFlagByteLength(4 * n);

        buf.ensureCapacity(initialCapacity);

        // The byte offset of the start of the coded record in the buffer.
        final int O_origin = buf.pos();
        
        final int size = raba.size();

        buf.putByte(VERSION0);

        buf.putInt(size);

        for (int i = 0; i < size; i += 2) {

            final byte lowNibble = (raba.isNull(i) ? 7 : raba.get(i)[0]);

            final byte highNibble = (i + 1 == size ? 0
                    : ((raba.isNull(i + 1) ? 7 : raba.get(i + 1)[0])));

            final byte b = (byte) (0xff & (highNibble << 4 | lowNibble));

            buf.putByte(b);
            
        }

        // slice on just the coded data record.
        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

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
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data,
                final int size) {

            this.data = data;

            this.size = size;
            
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

            final byte version = data.getByte(0/* offset */);

            if (version != VERSION0) {

                throw new RuntimeException("Unknown version=" + version);

            }

            size = data.getInt(1/* offset */);

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

            final byte b = data.getByte(O_values + index / 2);

            final int t = (index % 2 == 0) ? (b & 0x0f) : (b >> 4);

            return (byte) (0xff & t);

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
