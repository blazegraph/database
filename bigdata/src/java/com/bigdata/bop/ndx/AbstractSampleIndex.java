/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.ndx;


import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.BOp.Annotations;
import com.bigdata.btree.IIndex;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Abstract base class for sampling operator for an {@link IIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements materialized from that index.
 * 
 * @todo Implement sample operator. E.g., sampleRange(fromKey,toKey,limit). This
 *       could be on {@link IIndex} or on {@link IAccessPath}. For a shard view,
 *       it must proportionally select from among the ordered components of the
 *       view. For a hash table it would be sample(limit) since range based
 *       operations are not efficient.
 *       <p>
 *       This should accept an index, not a predicate (for RDF we determine the
 *       index an analysis of the bound and unbound arguments on the predicate
 *       and always have a good index, but this is not true in the general
 *       case). When the index is remote, it should be executed at the remote
 *       index.
 * 
 * @todo This needs to operation on element chunks, not {@link IBindingSet}
 *       chunks. It also may not require pipelining.
 */
abstract public class AbstractSampleIndex<E> extends AbstractPipelineOp<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Known annotations.
     */
    public interface Annotations extends BOp.Annotations {
        /**
         * The sample limit.
         */
        String LIMIT = "limit";
    }

    protected AbstractSampleIndex(final IPredicate<E> pred, final int limit) {

        super(new BOp[] { pred }, NV.asMap(new NV[] {//
                new NV(Annotations.LIMIT, Integer.valueOf(limit)) //
                }));

        if (pred == null)
            throw new IllegalArgumentException();

        if (limit <= 0)
            throw new IllegalArgumentException();
        
    }
    
    @SuppressWarnings("unchecked")
    public IPredicate<E> pred() {
        
        return (IPredicate<E>) args[0];
        
    }

    public int limit() {
     
        return (Integer) annotations.get(Annotations.LIMIT);
        
    }

}
