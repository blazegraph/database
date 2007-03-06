/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.isolation;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.objndx.IValueSerializer;

/**
 * A persistence capable implementation of {@link IValue}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Value implements IValue {

    final boolean deleted;

    final short versionCounter;

    final byte[] datum;

    Value(short versionCounter, boolean deleted, byte[] value) {

        if (versionCounter < 0) {
            
            throw new IllegalArgumentException("versionCounter is negative");
            
        }

        if(deleted && value != null) {
            
            throw new IllegalArgumentException("deleted, but datum is non-null");

        }
        
        this.versionCounter = versionCounter;

        this.deleted = deleted;

        this.datum = value;

    }

    final public short getVersionCounter() {

        return versionCounter;

    }

    final public short nextVersionCounter() {

        int nextVersionCounter = versionCounter + 1;

        if (nextVersionCounter > Short.MAX_VALUE) {

            return ROLLOVER_VERSION_COUNTER;

        }

        return (short) nextVersionCounter;

    }

    final public boolean isDeleted() {

        return deleted;

    }

    //        public boolean isFirstVersion() {
    //
    //            // first version, whether or not it is deleted.
    //            return versionCounter == 0;
    //
    //        }

    final public byte[] getValue() {

        return datum;

    }

    /**
     * Dumps the state of the entry.
     */
    public String toString() {

        return "{versionCounter=" + getVersionCounter() + ", deleted="
                + isDeleted() + ", datum=" + Arrays.toString(datum) + "}";

    }

    /**
     * (De-)serializer for the {@link Value} objects used to support deletion
     * markers and transactional isolation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo explore compression techniques, most likely dictionary encodings or
     *       hamming encodings.
     */
    public static class Serializer implements IValueSerializer {

        private static final long serialVersionUID = 6322667187129464088L;

        public static final transient int VERSION0 = 0x0;

        public transient static final Serializer INSTANCE = new Serializer();
        
        public Serializer() {}

        public void getValues(DataInputStream is, Object[] values, int n)
        throws IOException {

            final int version = (int)LongPacker.unpackLong(is);

            if (version != VERSION0)
                throw new IOException("Unknown version=" + version);

            for (int i = 0; i < n; i++) {

                final short versionCounter = ShortPacker.unpackShort(is);
                
                // Note: substract (2) to get the true length. -1 is a null
                // datum. -2 is a deleted datum.
                final long len = LongPacker.unpackLong(is) - 2;
                
                final boolean deleted = len == -2;
                
                // we fill in the byte[] datum below.
                if(deleted) {

                    values[i] = new Value(versionCounter,true,null);
                    
                } else if (len == -1) {

                    // the datum is null vs an empty byte[].
                    values[i] = new Value(versionCounter,deleted,null);
                    
                } else {

                    // The datum is a byte[] of any length, including zero.
                    values[i] = new Value(versionCounter,deleted,new byte[(int)len]);
                    
                }

            }

            /*
             * Read in the byte[] for each datum.
             */
            for( int i=0; i<n; i++) {
            
                Value value = (Value)values[i];
                
                if( value.deleted || value.datum==null) continue;
                
                is.read(value.datum, 0, value.datum.length);
                
            }
            
        }

        public void putValues(DataOutputStream os, Object[] values, int n)
                throws IOException {

            /*
             * Buffer lots of single byte operations. Estimate #of bytes in the
             * buffer as at most 2 bytes for the version counters plus at most 4
             * bytes for each byte count. This will always be an overestimate
             * but it means that we never grow [baos].
             */
            {

                final int size = 2 + n * 2 + n * 4; // est of buffer capacity.
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream(size);

                DataOutputStream dbaos = new DataOutputStream(baos);

                LongPacker.packLong(dbaos, VERSION0);

                for (int i = 0; i < n; i++) {

                    Value value = (Value) values[i];

                    ShortPacker.packShort(dbaos,value.versionCounter);
                    
                    final long len;
                    
                    if(value.deleted) {

                        // A deleted datum is indicated by a len of -2. 
                        len = -2;
                        
                    } else {
                        
                        // Note: a null datum is indicated by -1 length.
                        len = (value.datum==null?-1:value.datum.length);
                       
                    }

                    // Note: we add (2) so that the length is always
                    // non-negative so that we can pack it.
                    LongPacker.packLong(dbaos,len+2);
                    
                }

                dbaos.flush();
                
                os.write(baos.toByteArray());
                
            }

            /*
             * but do not buffer batch byte operations.
             */
            for (int i = 0; i < n; i++) {

                final byte[] value = ((Value) values[i]).datum;

                if(value!= null) {

                    os.write(value, 0, value.length);
                    
                }
                
            }
            
        }

    }

}
