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
package com.bigdata.objndx;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

/**
 * A non-persistence capable implementation of {@link IObjectIndexEntry}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Value implements IValue {

    final boolean deleted;

    final short versionCounter;

    final byte[] value;

    Value(short versionCounter, boolean deleted, byte[] value) {

        assert versionCounter >= 0;

        this.versionCounter = versionCounter;

        this.deleted = deleted;

        this.value = value;

    }

    public short getVersionCounter() {

        return versionCounter;

    }

    public short nextVersionCounter() {

        int nextVersionCounter = versionCounter + 1;

        if (nextVersionCounter > Short.MAX_VALUE) {

            return (short) 1;

        }

        return (short) nextVersionCounter;

    }

    public boolean isDeleted() {

        return deleted;

    }

    //        public boolean isFirstVersion() {
    //
    //            // first version, whether or not it is deleted.
    //            return versionCounter == 0;
    //
    //        }

    public byte[] getValue() {

        return value;

    }

    /**
     * Dumps the state of the entry.
     */
    public String toString() {

        return "{versionCounter=" + getVersionCounter() + ", deleted="
                + isDeleted() + ", value=" + Arrays.toString(value) + "}";

    }

    /**
     * (De-)serializer for the {@link Value} objects used to support deletion
     * markers and transactional isolation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo write a test suite.
     * 
     * @todo Is it worth refactoring such that the deleted flags could be
     *       migrated into a run-length encoding and we could then use the
     *       {@link ShortPacker} on the version counter?
     * 
     * @todo explore compression techniques.
     */
    public static class Serializer implements IValueSerializer {

        private static final long serialVersionUID = 6322667187129464088L;

        public static final transient int VERSION0 = 0x0;

        public transient static final Serializer INSTANCE = new Serializer();
        
        public Serializer() {}

        public void getValues(DataInputStream is, Object[] values, int n)
        throws IOException {

            final int version = is.readInt();

            if (version != VERSION0)
                throw new IOException("Unknown version=" + version);

            for (int i = 0; i < n; i++) {

                short versionCounter = is.readShort();
                
                final int len = (int)LongPacker.unpackLong(is);
                
                boolean deleted = false;
                
                if(versionCounter<0) {
                    
                    versionCounter = (short)-versionCounter;
                    
                    deleted = true;
                    
                }

                // we fill in the byte[] value below.
                values[i] = new Value(versionCounter,deleted,new byte[len]);

            }

            /*
             * Read in the byte[] for each value.
             */
            for( int i=0; i<n; i++) {
            
                final byte[] value = ((Value) values[i]).value;
                
                is.read(value, 0, value.length);
                
            }
            
        }

        public void putValues(DataOutputStream os, Object[] values, int n)
                throws IOException {

            os.writeInt(VERSION0);

            /*
             * Buffer lots of single byte operations. Estimate #of bytes in the
             * buffer as 2 bytes for the version counters plus at most 4 bytes
             * for each byte count.
             */
            {

                final int size = n * 2 + n * 4; // est of buffer capacity.
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream(size);

                DataOutputStream dbaos = new DataOutputStream(baos);

                for (int i = 0; i < n; i++) {

                    Value value = (Value) values[i];

                    short versionCounter = value.versionCounter;
                    
                    if(value.deleted) {
                        
                        versionCounter = (short)-versionCounter;
                        
                    }
                    
                    dbaos.writeShort(versionCounter);

                    final int len = (value.value==null?0:value.value.length);
                    
                    LongPacker.packLong(dbaos,len);

                }

                dbaos.flush();
                
                os.write(baos.toByteArray());
                
            }

            /*
             * but do not buffer batch byte operations.
             */
            for (int i = 0; i < n; i++) {

                final byte[] value = ((Value) values[i]).value;

                if(value!= null) {

                    os.write(value, 0, value.length);
                    
                }
                
            }
            
        }

    }

}
