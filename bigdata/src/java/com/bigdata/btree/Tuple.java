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
 * Created on Dec 12, 2006
 */

package com.bigdata.btree;


/**
 * A key-value pair used to facilitate some iterator constructs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Tuple<E> extends AbstractTuple<E> {

//    private final WeakReference<AbstractBTree> btreeRef;
//    
//    /**
//     * The {@link AbstractBTree} specified to the ctor.
//     */
//    final public AbstractBTree getBTree() {
//        
//        return btreeRef.get();
//        
//    }
    
    private final ITupleSerializer tupleSer;

    /**
     * 
     * Note: This was modified to not hold a reference to the
     * {@link AbstractBTree} since we use {@link Tuple} in {@link ThreadLocal}
     * constructions. Instead, it just obtains a reference to the
     * {@link ITupleSerializer} and holds onto that.
     * 
     * @param btree
     * @param flags
     */
    public Tuple(final AbstractBTree btree, final int flags) {

        super(flags);

        if (btree == null)
            throw new IllegalArgumentException();

        this.tupleSer = btree.getIndexMetadata().getTupleSerializer();
        
    }

    public int getSourceIndex() {
        
        return 0;
        
    }
    
    public ITupleSerializer getTupleSerializer() {

        return tupleSer;
        
    }
    
}
