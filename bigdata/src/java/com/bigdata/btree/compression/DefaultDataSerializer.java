package com.bigdata.btree.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.raba.IRandomAccessByteArray;


/**
 * No compression.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultDataSerializer implements IDataSerializer, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -7960255442029142379L;

    public transient static DefaultDataSerializer INSTANCE = new DefaultDataSerializer();

    /**
     * Sole constructor (handles de-serialization also).
     */
    public DefaultDataSerializer() {

    }
    
    public void read(final DataInput in,
            final IRandomAccessByteArray raba) throws IOException {
//            final boolean notNull = in.readBoolean();
//            if (!notNull) {
//                return null;
//            }
        final int n = (int) LongPacker.unpackLong(in);
        for (int i = 0; i < n; i++) {
            // when lenPlus == 0 the value is null (vs byte[0]).
            final int lenPlus1 = (int) LongPacker.unpackLong(in);
            if (lenPlus1 > 0) {
                byte[] tmp = new byte[lenPlus1 - 1];
                in.readFully(tmp);
                raba.add(tmp);
            } else
                raba.add(null);
        }
//            return new RandomAccessByteArray(0/*fromIndex*/,n/*toIndex*/,a);
    }

    public void write(final DataOutput out, final IRandomAccessByteArray raba)
            throws IOException {
        final int n = raba.size();
//            if (a == null && n != 0)
//                throw new IllegalArgumentException();
//            out.writeBoolean(a != null); // notNull
//            if (a != null) {
            LongPacker.packLong(out, n);
//                for (int i = fromIndex; i < toIndex; i++) {
//                    final byte[] e = a[i];
//                for(final byte[] e : raba) {
              for(int i=0; i<n; i++) {
                  // differentiate a null value from an empty byte[].
                  final boolean isNull = raba.isNull(i);
                  final int lenPlus1 = isNull ? 0 : raba.length(i) + 1;
                  LongPacker.packLong(out, lenPlus1);
                  if (!isNull) {
                if (out instanceof OutputStream) {
                    raba.copy(i, (OutputStream) out);
                } else {
                    /*
                     * @todo Normally the DataOutput instance extends
                     * OutputStream. If that is not the case for some execution
                     * context then we really need to have parallel methods for
                     * DataOutput on raba#copy() so this can be efficient (avoid
                     * the byte[] allocation) for that execution context.
                     */
                    out.write(raba.get(i));
                }
            }
            }
//            }
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        // NOP
        
    }

}
