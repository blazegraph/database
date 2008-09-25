package com.bigdata.rdf.spo;

import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.compression.IRandomAccessByteArray;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * We encode the value in 3 bits per statement. The 1st bit is the override
 * flag. The remaining two bits are the statement type {inferred, explicit, or
 * axiom}. The value b111 is used as a placeholder for a deleted index entry and
 * will be present iff delete markers are used by the index - it de-serializes
 * to a [null].
 * <p>
 * Note: the 'override' flag is NOT stored in the statement indices, but it is
 * passed by the procedure that writes on the statement indices so that we can
 * decide whether or not to override the type when the statement is pre-existing
 * in the index.
 * <p>
 * Note: this procedure can not be used
 * {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} are enabled.
 * 
 * @todo test suite.
 * 
 * @see StatementEnum
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FastRDFValueCompression implements IDataSerializer, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1933430721504168533L;

    /**
     * Sole constructor (handles de-serialization also).
     */
    public FastRDFValueCompression() {

    }

    public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {

        InputBitStream ibs = new InputBitStream((InputStream) in, 0/* unbuffered! */);

        /*
         * read the values.
         */
        
        final int n = ibs.readNibble();

        for (int i = 0; i < n; i++) {

            int b = ibs.readInt(3);
            
            if (b == 7) {
             
                // A deleted value.
                
                raba.add(null);
                
            } else {
            
                raba.add(new byte[] { (byte) b });
                
            }

        }
        
    }

    public void write(DataOutput out, IRandomAccessByteArray raba) throws IOException {

        final OutputBitStream obs = new OutputBitStream((OutputStream) out, 0 /* unbuffered! */);

        /*
         * write the values.
         */

        final int n = raba.getKeyCount();
        
        assert n >= 0;
        
        obs.writeNibble(n);

        for (int i = 0; i < n; i++) {

            if (raba.isNull(i)) {

                // flag a deleted value (de-serialize to a null).
                obs.writeInt( 7, 3 );
                
            } else {

                final byte[] val = raba.getKey(i);

                obs.writeInt((int) val[0], 3);
                
            }

        }

        /*
         * Note: ALWAYS flush!
         */
        obs.flush();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        // NOP
        
    }

}