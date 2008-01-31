/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 28, 2008
 */

package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.IIndexProcedure.IKeyRangeIndexProcedure;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractKeyRangeIndexProcedure implements
        IKeyRangeIndexProcedure, Externalizable {

    protected byte[] fromKey;

    protected byte[] toKey;

    /**
     * De-serialization ctor.
     */
    public AbstractKeyRangeIndexProcedure() {
        
    }
    
    public AbstractKeyRangeIndexProcedure(byte[] fromKey,byte[] toKey) {
        
        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        readKeys(in);
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        writeKeys(out);
        
    }

    private static final short VERSION0 = 0x0;

    protected void readKeys(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = in.readShort();

        if (version != VERSION0) {

            throw new IOException("Unknown version: " + version);
            
        }
     
        // fromKey
        {
            
            final int len = in.readInt();
            
            if(len > 0) {
                
                fromKey = new byte[len - 1];
                
                in.readFully(fromKey);
                
            }
            
        }

        // toKey
        {
            
            final int len = in.readInt();
            
            if(len > 0) {
                
                toKey = new byte[len - 1];
                
                in.readFully(toKey);
                
            }

        }

    }
    
    protected void writeKeys(ObjectOutput out) throws IOException {
        
        out.writeShort(VERSION0);

        /*
         * Note: 0 indicates a null reference. Otherwise the length of the
         * byte[] is written as (len + 1).
         */

        out.writeInt(fromKey == null ? 0 : fromKey.length + 1);

        if (fromKey != null) {

            out.write(fromKey);

        }

        out.writeInt(toKey == null ? 0 : toKey.length + 1);

        if (toKey != null) {

            out.write(toKey);

        }


    }
}
