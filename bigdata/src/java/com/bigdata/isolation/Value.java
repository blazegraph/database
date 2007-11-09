/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.isolation;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.IValueSerializer;
import com.bigdata.io.DataOutputBuffer;

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

        public void getValues(DataInput is, Object[] values, int n)
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
                
                is.readFully(value.datum, 0, value.datum.length);
                
            }
            
        }

        public void putValues(DataOutputBuffer os, Object[] values, int n)
                throws IOException {

            /*
             * Buffer lots of single byte operations. Estimate #of bytes in the
             * buffer as at most 2 bytes for the version counters plus at most 4
             * bytes for each byte count. This will always be an overestimate
             * but it means that we never grow [baos].
             */
            {

//                final int size = 2 + n * 2 + n * 4; // est of buffer capacity.
//                
//                ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
//
//                DataOutputStream dbaos = new DataOutputStream(baos);
//
//                LongPacker.packLong(dbaos, VERSION0);
                
                os.packLong(VERSION0);

                for (int i = 0; i < n; i++) {

                    Value value = (Value) values[i];

//                    ShortPacker.packShort(dbaos,value.versionCounter);
                    os.packShort(value.versionCounter);
                    
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
//                    LongPacker.packLong(dbaos,len+2);
                    os.packLong(len+2);
                    
                }

//                dbaos.flush();
//                
//                os.write(baos.toByteArray());
                
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
