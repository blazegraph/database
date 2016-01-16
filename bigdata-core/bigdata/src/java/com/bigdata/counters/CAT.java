/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on May 11, 2010
 */

package com.bigdata.counters;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;

import org.cliffc.high_scale_lib.ConcurrentAutoTable;

/**
 * An alias for the high-scale-lib counter implementation. {@link CAT}s are
 * useful when a counter is extremely hot for updates and adaptively expand
 * their internal state to minimize thread contention. However, they can do more
 * work than an {@link AtomicLong} on read since they must scan an internal
 * table to aggregate updates from various threads.
 * <p>
 * Note: The class uses a delegation pattern in order to replace the
 * serialization logic with a simple serialized long value.
 * 
 * @see ConcurrentAutoTable
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CAT /*extends ConcurrentAutoTable*/ implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private ConcurrentAutoTable cat;

    /**
     * Initialize a new counter with a value of <code>0L</code>.
     */
    public CAT() {
        cat = new ConcurrentAutoTable();
    }

    /**
     * Add the value to the counter.
     * 
     * @see ConcurrentAutoTable#add(long)
     */
    final public void add(long x) {
        cat.add(x);
    }

    /** Decrement the value of the counter. 
     * 
     * @see ConcurrentAutoTable#decrement()
     */
    final public void decrement() {
        cat.decrement();
    }

    /**
     * Increment the value of the counter.
     * 
     * @see ConcurrentAutoTable#increment()
     */
    final public void increment() {
        cat.increment();
    }

    /**
     * Set the value of the counter.
     * 
     * @see ConcurrentAutoTable#set(long)
     */
    final public void set(long x) {
        cat.set(x);
    }

    /**
     * Current value of the counter.
     * 
     * @see ConcurrentAutoTable#get()
     */
    final public long get() {
        return cat.get();
    }

    /**
     * Estimate the current value of the counter.
     * 
     * @see ConcurrentAutoTable#estimate_get()
     */
    final public long estimate_get() {
        return cat.estimate_get();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        
        out.writeLong(get());
        
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        cat.set(in.readLong());
        
    }

    /**
     * Return the current value of the counter.
     */
    @Override
    public String toString() {
    	
    	return Long.toString(cat.get());
    	
    }
    
}
