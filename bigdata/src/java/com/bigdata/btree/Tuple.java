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

import java.lang.ref.WeakReference;


/**
 * A key-value pair used to facilitate some iterator constructs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Tuple<E> extends AbstractTuple<E> {

    /**
     * This is used to lazily obtain the {@link ITupleSerializer} since it often
     * can not be immediately materialized in the context in which
     * {@link Tuple#Tuple(AbstractBTree, int)} is invoked.
     */
    private final WeakReference<AbstractBTree> btreeRef;
    
    /**
     * 
     * @param btree
     * @param flags
     */
    public Tuple(final AbstractBTree btree, final int flags) {

        super(flags);

        if (btree == null)
            throw new IllegalArgumentException();

        this.btreeRef = new WeakReference<AbstractBTree>(btree);
        
    }

    public int getSourceIndex() {
        
        return 0;
        
    }

    /**
     * This is lazily resolved from a {@link WeakReference} to the
     * {@link AbstractBTree}. It is a runtime error if the {@link AbstractBTree}
     * reference has been cleared, but then you should not be using a
     * {@link Tuple} instance after the {@link AbstractBTree} for which it was
     * created has been cleared.
     */
    public ITupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            synchronized (this) {

                final AbstractBTree btree = btreeRef.get();

                if (btree == null) {

                    throw new AssertionError("Reference cleared");

                }

                tupleSer = btree.getIndexMetadata().getTupleSerializer();

            }
            
        }

        return tupleSer;
        
    }

    // used by double-checked locking pattern.
    private volatile ITupleSerializer tupleSer = null;
    
}
