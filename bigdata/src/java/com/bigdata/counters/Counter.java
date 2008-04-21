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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

/**
 * A named counter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 *            The data type for the counter value.
 */
final class Counter<T> implements ICounter {
    
    ICounterSet parent;
    
    private final String name;
    
    private final IInstrument<T> instrument;
    
    Counter(ICounterSet parent, String name, IInstrument<T> instrument) {
        
        if (parent == null)
            throw new IllegalArgumentException();
        
        if (name == null)
            throw new IllegalArgumentException();
        
        if (instrument == null)
            throw new IllegalArgumentException();
        
        this.parent = parent;
        
        this.name = name;
        
        this.instrument = instrument;
        
    }

    public ICounterSet getParent() {
        
        return parent;
        
    }
    
    public String getName() {
        
        return name;
        
    }
    
    public String getPath() {
        
        if(parent.isRoot()) {
            
            /*
             * Handles: "/foo", where "foo" is this counter.
             */
            return parent.getPath()+name;
            
        }
        
        // Handles all other cases.
        return parent.getPath() + ICounterSet.pathSeparator + name;
        
    }
    
    public int getDepth() {
        
        int depth = 0;
        
        ICounterNode t = this;
        
        while(!t.isRoot()) {
            
            t = t.getParent();
            
            depth++;
            
        }
        
        return depth;
        
    }
    
    public String toString() {
        
        return getPath();
        
    }

    public ICounterSet getRoot() {

        return parent.getRoot();
        
    }

    public boolean isRoot() {

        return false;
        
    }

    /**
     * Invokes {@link Instrument#getValue()}
     */
    public T getValue() {
        
        return instrument.getValue();
        
    }
    
    /**
     * Invokes {@link Instrument#setValue(Object, long)}.
     */
    public void setValue(Object value, long timestamp) {
        
        instrument.setValue((T)value, timestamp);
        
    }
    
    /**
     * Invokes {@link Instrument#lastModified()}
     */
    public long lastModified() {
        
        return instrument.lastModified();
        
    }
    
    /**
     * Returns <code>false</code>.
     */
    final public boolean isCounterSet() {
        
        return false;
        
    }

    /**
     * Returns <code>true</code>
     */
    final public boolean isCounter() {
        
        return true;
        
    }

    /**
     * Always returns <code>null</code> since there are no children.
     */
    public ICounterNode getChild(String name) {
        
        return null;
        
    }

    /**
     * Always returns <code>null</code> since there are no children.
     */
    public ICounterNode getPath(String path) {

        return null;
        
    }
    
    public IInstrument<T> getInstrument() {
        
        return instrument;
        
    }
    
}
