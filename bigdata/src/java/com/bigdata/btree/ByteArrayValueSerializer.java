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
/*
 * Created on Feb 14, 2007
 */
package com.bigdata.btree;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.IConflictResolver;

/**
 * An serializer for byte[] values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo this is only used to store values produced by an
 *       {@link IConflictResolver} at this time. if it is used for more
 *       general purposes then compress techniques should be considered.
 */
public class ByteArrayValueSerializer implements IValueSerializer {

    private static final long serialVersionUID = -4144492641187167035L;

    public final static transient IValueSerializer INSTANCE = new ByteArrayValueSerializer();
    
    public static final transient int VERSION0 = 0x0;

    public void putValues(DataOutputBuffer os, Object[] values, int n) throws IOException {

        /*
         * Buffer lots of single byte operations.
         */
        {

//            final int size = 2 + n * 2; // est of buffer capacity.
//            
//            ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
//
//            DataOutputStream dbaos = new DataOutputStream(baos);
//
//            LongPacker.packLong(dbaos,VERSION0);
            
            os.packLong(VERSION0);
            
            for (int i = 0; i < n; i++) {

                final byte[] value = (byte[])values[i];
                
                final long len = value == null ? -1 : value.length;

                // Note: we add (1) so that the length is always
                // non-negative so that we can pack it.
//                LongPacker.packLong(dbaos,len+1);
                os.packLong(len+1);
                
            }

//            dbaos.flush();
//            
//            os.write(baos.toByteArray());
            
        }

        /*
         * but do not buffer batch byte operations.
         */
        for (int i = 0; i < n; i++) {

            final byte[] value = ((byte[]) values[i]);

            if (value != null) {

                os.write(value, 0, value.length);
                
            }
            
        }
        
    }
    
    public void getValues(DataInput is, Object[] values, int n) throws IOException {

        final int version = (int)LongPacker.unpackLong(is);

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        for (int i = 0; i < n; i++) {

            // Note: substract (1) to get the true length. -1 is a null
            // datum.
            final long len = LongPacker.unpackLong(is) - 1;
            
            if (len == -1) {

                values[i] = null;
                
            } else {

                // The datum is a byte[] of any length, including zero.
                values[i] = new byte[(int) len];
                
            }

        }

        /*
         * Read in the byte[] for each datum.
         */
        for( int i=0; i<n; i++) {
        
            final byte[] value = (byte[])values[i];
            
            if( value==null) continue;
            
            is.readFully(value, 0, value.length);
            
        }

    }

}