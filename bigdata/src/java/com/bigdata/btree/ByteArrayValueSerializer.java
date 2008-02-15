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
/*
 * Created on Feb 14, 2007
 */
package com.bigdata.btree;

import java.io.DataInput;
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

    public void putValues(DataOutputBuffer os, byte[][] values, int n) throws IOException {

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

                final byte[] value = values[i];
                
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
    
    public void getValues(DataInput is, byte[][] values, int n) throws IOException {

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
        
            final byte[] value = values[i];
            
            if (value == null)
                continue;
            
            is.readFully(value, 0, value.length);
            
        }

    }

}