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
package com.bigdata.rdf.serializers;

import java.io.DataInput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IValueSerializer;
import com.bigdata.io.DataOutputBuffer;

/**
 * The value is a <code>long</code> integer that is the term identifier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class TermIdSerializer implements IValueSerializer {

    private static final long serialVersionUID = 8081006629809857019L;
    
    public static transient final IValueSerializer INSTANCE = new TermIdSerializer();
    
    /**
     * Note: It is faster to use packed longs, at least on write with test
     * data (bulk load of wordnet nouns).
     */
    final static boolean packedLongs = true;
    
    public TermIdSerializer() {}
    
    public void getValues(DataInput is, Object[] values, int n)
            throws IOException {

        for(int i=0; i<n; i++) {
            
            if (packedLongs) {

                values[i] = Long.valueOf(LongPacker.unpackLong(is));

            } else {

                values[i] = Long.valueOf(is.readLong());

            }
            
        }
        
    }

    public void putValues(DataOutputBuffer os, Object[] values, int n)
            throws IOException {

        for(int i=0; i<n; i++) {

            if(packedLongs) {

//                LongPacker.packLong(os, ((Long) values[i]).longValue());
                os.packLong(((Long) values[i]).longValue());
                
            } else {

                os.writeLong(((Long) values[i]).longValue());
            
            }
            
        }
        
    }
    
}