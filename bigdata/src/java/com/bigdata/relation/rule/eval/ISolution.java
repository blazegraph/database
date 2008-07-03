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
 * Created on Jun 24, 2008
 */

package com.bigdata.relation.rule.eval;

import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;

/**
 * A solution bundles together any of (a) the materialized element corresponding
 * to the bindings on the head of an {@link IRule}; (b) the {@link IBindingSet}
 * used to generate that solution; and (c) the {@link IRule} from which those
 * bindings were computed. All data are optional. Which data are included
 * depends on a set of bit flags known to the {@link IJoinNexus} implementation
 * and the behavior of its {@link IJoinNexus#newSolution(IRule, IBindingSet)}
 * implementation.
 * <p>
 * Note: The {@link IBindingSet} is useful both for high-level query and to
 * materialize the justifications for the entailments. The RDF DB uses this for
 * to maintain a justifications index in support of truth maintenance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISolution<E> {

    /**
     * Return the element materialized from the head of the rule given a set of
     * {@link IBindingSet bindings} for that rule (optional operation).
     * 
     * @return The element -or- <code>null</code> if the element was not
     *         requested.
     */
    public E get();
    
    /**
     * Return the {@link IRule} that generated this solution (optional
     * operation).
     * 
     * @return The {@link IRule} -or- <code>null</code> if the rule was not
     *         requested.
     */
    public IRule getRule();
    
    /**
     * Return the {@link IBindingSet} for this solution (optional operation).
     * 
     * @return The {@link IBindingSet}.
     * 
     * @return The {@link IBindingSet} -or- <code>null</code> if the binding
     *         set was not requested.
     */
    public IBindingSet getBindingSet();
    
}
