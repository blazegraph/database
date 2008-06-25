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

package com.bigdata.join;

/**
 * A solution bundles together the materialized element corresponding to the
 * bindings on the head of an {@link IRule} with the {@link IBindingSet} used to
 * generate that solution. For efficiency, the {@link IBindingSet} is often not
 * included. This is an {@link IProgram} option.
 * <p>
 * Note: One use for the {@link IBindingSet}s is to materialize the
 * justifications for the entailments. The RDF DB uses this for to maintain a
 * justifications index in support of truth maintenance.
 * 
 * FIXME It may be easier to always clone and materalize the binding set rather
 * than the element and let the client extract what they want, but we still want
 * the rule available as metadata in either case.
 * 
 * FIXME JOINs need to share some bindings with sub-JOINS, but not the full
 * binding set. Consider this when attending to efficient serialization. Will
 * there be a JOIN operation on the data service itself?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISolution<E> {

    /**
     * Return the element materialized from the head of the rule given a set of
     * {@link IBindingSet bindings} for that rule. The rule MAY be available
     * from {@link #getRule()}. The bindings MAY be available from
     * {@link #getBindingSet()}.
     * 
     * @return The element.
     */
    public E get();
    
    /**
     * Return the {@link IRule} that generated this solution (optional
     * operation).
     * 
     * @return The {@link IRule} -or- <code>null</code> if only the element
     *         was requested when the rule was executed.
     */
    public IRule getRule();
    
    /**
     * Return the {@link IBindingSet} for this solution (optional operation).
     * 
     * @return The {@link IBindingSet}.
     * 
     * @return The {@link IBindingSet} -or- <code>null</code> if only the
     *         element was requested when the rule was executed.
     */
    public IBindingSet getBindingSet();
    
}
