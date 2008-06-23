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

package com.bigdata.join.rdf;

import java.util.Comparator;

import com.bigdata.join.IKeyOrder;

/**
 * Represents the key order used by an index for a triple relation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class KeyOrder implements IKeyOrder<ISPO> {

    /**
     * The index whose keys are formed with the {s,p,o} ordering of the triple.
     */
    public static final transient KeyOrder SPO = new KeyOrder(0,"SPO");

    /**
     * The index whose keys are formed with the {p,o,s} ordering of the triple.
     */
    public static final transient KeyOrder POS = new KeyOrder(1,"POS");

    /**
     * The index whose keys are formed with the {o,s,p} ordering of the triple.
     */
    public static final transient KeyOrder OSP = new KeyOrder(2,"OSP");

    private final int i;
    private final String name;

    private KeyOrder(int i,String name) {

        this.i = i;
        
        this.name = name;

    }

    /**
     * The base name for the index.
     */
    public String getName() {

        return name;
        
    }
    
    /**
     * The integer used to represent the {@link KeyOrder} according to the
     * following table:
     * <dl>
     * <dt>ZERO (0)</dt>
     * <dd>{@link #SPO}</dd>
     * <dt>ONE (1)</dt>
     * <dd>{@link #POS}</dd>
     * <dt>TWO (2)</dt>
     * <dd>{@link #OSP}</dd>
     * </dl>
     * 
     * @return
     */
    public int index() {
        
        return i;
        
    }
    
    /**
     * Return the comparator that places {@link ISPO}s into the natural order
     * for the associated index.
     */
    final public Comparator<ISPO> getComparator() {

        switch (i) {
        case 0:
            return SPOComparator.INSTANCE;
        case 1:
            return POSComparator.INSTANCE;
        case 2:
            return OSPComparator.INSTANCE;
        default:
            throw new IllegalArgumentException("Unknown: " + this);
        }

    }

}
