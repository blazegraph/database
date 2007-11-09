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
package com.bigdata.rdf.inf;

import java.io.DataInput;
import java.io.IOException;

import com.bigdata.btree.IValueSerializer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Note: No data is associated with the keys in the justifications index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JustificationSerializer implements IValueSerializer {

    private static final long serialVersionUID = -2174985132435709536L;

    public static transient final IValueSerializer INSTANCE = new JustificationSerializer();

    public JustificationSerializer() {
    }

    public void getValues(DataInput is, Object[] values, int n)
            throws IOException {
        
    }

    /**
     * This serializer just casts the Object[] to a byte[][] on serialization
     * and writes out those bytes.
     */
    public void putValues(DataOutputBuffer os, Object[] values, int n)
            throws IOException {
        
    }

}
