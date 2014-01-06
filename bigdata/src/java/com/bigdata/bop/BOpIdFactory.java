/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
package com.bigdata.bop;

import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * A factory which may be used when some identifiers need to be reserved.
 */
public class BOpIdFactory implements IdFactory {
	
    /** The set of reserved bop identifiers. */
    private LinkedHashSet<Integer> ids;

    private int nextId = 0;

    /**
     * Reserve a bop id by adding it to a set of known identifiers that will not
     * be issued by {@link #nextId()}.
     * 
     * @param id
     *            The identifier.
     */
    public void reserve(final int id) {
       
        synchronized (this) {
        
            if (ids == null) {

                // Lazily allocated.
                ids = new LinkedHashSet<Integer>();

                ids.add(id);

            }
            
        }
        
    }

    @Override
    public int nextId() {

        synchronized (this) {

            if (ids != null) {

                while (ids.contains(nextId)) {

                    nextId++;

                }

            }

            return nextId++;

        }

    }

    /**
     * Reserve ids used by the predicates or constraints associated with some
     * join graph.
     * 
     * @param preds
     *            The vertices of the join graph.
     * @param constraints
     *            The constraints of the join graph (optional).
     */
    public void reserveIds(final IPredicate<?>[] preds,
            final IConstraint[] constraints) {

        if (preds == null)
            throw new IllegalArgumentException();

        final BOpIdFactory idFactory = this;
        
        for (IPredicate<?> p : preds) {
        
            idFactory.reserve(p.getId());
            
        }

        if (constraints != null) {
        
            for (IConstraint c : constraints) {
                
                final Iterator<BOp> itr = BOpUtility
                        .preOrderIteratorWithAnnotations(c);

                while (itr.hasNext()) {
                    
                    final BOp y = itr.next();
                    
                    final Integer anId = (Integer) y
                            .getProperty(BOp.Annotations.BOP_ID);
                
                    if (anId != null)
                        idFactory.reserve(anId.intValue());
            
                }
    
            }

        }
    
    }

}