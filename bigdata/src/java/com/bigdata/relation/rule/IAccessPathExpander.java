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
 * Created on Sep 3, 2008
 */

package com.bigdata.relation.rule;

import java.io.Serializable;

import com.bigdata.bop.IPredicate;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * An interface for expander patterns for an {@link IPredicate} when it appears
 * in the right-hand position of a JOIN.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAccessPathExpander<E> extends Serializable {

    /**
     * Return the {@link IAccessPath} that will be used to evaluate the
     * {@link IPredicate}.
     * 
     * @param accessPath
     *            The {@link IAccessPath} that will be used by default.
     * 
     * @return The {@link IAccessPath} that will be used. You can return the
     *         given <i>accessPath</i> or you can layer additional semantics
     *         onto or otherwise override the given {@link IAccessPath}.
     */
    IAccessPath<E> getAccessPath(IAccessPath<E> accessPath);

    /**
     * Add the backchainer on top of the expander.
     * 
     * @return true if the backchainer should run
     * 
     * @deprecated Never <code>true</code>. The backchainer is only run for
     *             normal predicates in triples mode at this time. If it is to
     *             be layer, it should be layered as an annotation.  See
     *             https://sourceforge.net/apps/trac/bigdata/ticket/231.
     */
    boolean backchain();

    /**
     * If true, the predicate for this expander will be given priority in the
     * join order.
     * 
     * @return true if the predicate should be run first
     */
    boolean runFirst();
    
}
