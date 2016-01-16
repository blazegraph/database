/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Apr 29, 2013
 */
package com.bigdata.bop.join;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A "DISTINCT" filter for {@link IBindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IDistinctFilter {

    /**
     * The variables that are being projected out of the DISTINCT filter. The
     * solutions will be DISTINCT on this combination of variables. Bindings on
     * other variables will be dropped.
     */
    IVariable<?>[] getProjectedVars();
    
    /**
     * If the bindings are distinct for the configured variables then return a
     * new {@link IBindingSet} consisting of only the selected variables.
     * 
     * @param bset
     *            The binding set to be filtered.
     * 
     * @return A new {@link IBindingSet} containing only the distinct as bound
     *         values -or- <code>null</code> if the binding set duplicates a
     *         solution which was already accepted.
     */
    IBindingSet accept(final IBindingSet bset);

    /**
     * Vectored DISTINCT.
     * 
     * @param itr
     *            The source solutions.
     * @param stats
     *            Statistics object to be updated.
     * @param sink
     *            The sink onto which the DISTINCT solutions will be written.
     * @return The #of DISTINCT solutions.
     */
    long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink);
    
    /**
     * Discard the map backing this filter.
     */
    void release();

}
