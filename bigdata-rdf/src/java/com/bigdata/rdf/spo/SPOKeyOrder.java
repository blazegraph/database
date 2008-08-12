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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.spo;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Comparator;

import com.bigdata.striterator.IKeyOrder;

/**
 * Represents the key order used by an index for a triple relation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOKeyOrder implements IKeyOrder<SPO>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 87501920529732159L;
    
    /*
     * Note: these constants make it possible to use switch(index()) constructs.
     */
    public static final transient int _SPO = 0;
    public static final transient int _OSP = 1;
    public static final transient int _POS = 2;
    
    /**
     * The index whose keys are formed with the {s,p,o} ordering of the triple.
     */
    public static final transient SPOKeyOrder SPO = new SPOKeyOrder(_SPO,"SPO");

    /**
     * The index whose keys are formed with the {p,o,s} ordering of the triple.
     */
    public static final transient SPOKeyOrder POS = new SPOKeyOrder(_POS,"POS");

    /**
     * The index whose keys are formed with the {o,s,p} ordering of the triple.
     */
    public static final transient SPOKeyOrder OSP = new SPOKeyOrder(_OSP,"OSP");

    private final int index;

    private final String name;

    private SPOKeyOrder(final int index, final String name) {

        this.index = index;
        
        this.name = name;

    }

    /**
     * Returns the singleton corresponding to the <i>index</i>.
     * 
     * @param index
     *            The index.
     * 
     * @return The singleton {@link SPOKeyOrder} having that <i>index</i>.
     * 
     * @throws IllegalArgumentException
     *             if the <i>index</i> is not valid.
     */
    static public SPOKeyOrder valueOf(int index) {
        
        switch(index) {
        case _SPO:
            return SPO;
        case _POS:
            return POS;
        case _OSP:
            return OSP;
        default:
            throw new IllegalArgumentException("Unknown: index" + index);
        }
        
    }
    
    /**
     * The base name for the index.
     */
    public String getIndexName() {

        return name;
        
    }
    
    /**
     * Return {@link #getIndexName()}'s value.
     */
    public String toString() {
        
        return name;
        
    }
    
    /**
     * The integer used to represent the {@link SPOKeyOrder} which will be one of
     * the following symbolic constants: {@link #_SPO}, {@link #POS}, or
     * {@link #OSP}.
     */
    public int index() {
        
        return index;
        
    }
    
    /**
     * Return the comparator that places {@link ISPO}s into the natural order
     * for the associated index.
     */
    final public Comparator<SPO> getComparator() {

        switch (index) {
        case _SPO:
            return SPOComparator.INSTANCE;
        case _POS:
            return POSComparator.INSTANCE;
        case _OSP:
            return OSPComparator.INSTANCE;
        default:
            throw new IllegalArgumentException("Unknown: " + this);
        }

    }

    /**
     * Imposes the canonicalizing mapping during object de-serialization.
     */
    private Object readResolve() throws ObjectStreamException {

        return SPOKeyOrder.valueOf(index);

    }
    
}
