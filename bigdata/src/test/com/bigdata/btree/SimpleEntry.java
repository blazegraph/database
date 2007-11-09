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
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IAddressManager;


/**
 * Test helper provides an entry (aka value) for a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleEntry {

    private static int nextId = 1;

    private final int id;
    
    /**
     * Create a new entry.
     */
    public SimpleEntry() {
        
        id = nextId++;
        
    }

    public SimpleEntry(int id){
    
        this.id = id;
        
    }
    
    public int id() {
        
        return id;
        
    }
    
    public String toString() {
        
        return ""+id;
        
    }

    public boolean equals(Object o) {
        
        if( this == o ) return true;
        
        if( o == null ) return false;
        
        return id == ((SimpleEntry)o).id;
        
    }
    
    
    /**
     * (De-)serializer an array of {@link SimpleEntry}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Serializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = 4515322522558633041L;
        
        public transient static final Serializer INSTANCE = new Serializer();
        
        public Serializer() {}

        public void putValues(DataOutputBuffer os, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                os.writeInt(((SimpleEntry) values[i]).id);

            }

        }

        public void getValues(DataInput is, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                values[i] = new SimpleEntry(is.readInt());

            }

        }

    }

    /**
     * A (De-)serializer that always throws exceptions.  This is used when we
     * are testing in a context in which incremental IOs are disabled, e.g.,
     * by the {@link NoEvictionListener}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class NoSerializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = -6467578720380911380L;
        
        public transient static final NoSerializer INSTANCE = new NoSerializer();
        
        public NoSerializer() {}
        
        public void getValues(DataInput is, Object[] values, int n) throws IOException {

            throw new UnsupportedOperationException();
 
        }

        public void putValues(DataOutputBuffer os, Object[] values, int n) throws IOException {

            throw new UnsupportedOperationException();

        }

    }

}
