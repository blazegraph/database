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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.rules;

import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Base class for test suites for inference engine and the magic sets
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractInferenceEngineTestCase extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public AbstractInferenceEngineTestCase() {
    }

    /**
     * @param name
     */
    public AbstractInferenceEngineTestCase(String name) {
        super(name);
    }
    
    /**
     * Return the constant bound on the {@link Predicate} associated with the
     * {@link IAccessPath} at the specified slot index -or- {@link #NULL} iff
     * the predicate is not bound at that slot index.
     * 
     * @param ap
     *            The {@link IAccessPath}.
     * @param index
     *            The slot index on the {@link Predicate}.
     * 
     * @return Either the bound value -or- {@link #NULL} iff the index is
     *         unbound for the predicate for this access path.
     */
    @SuppressWarnings("rawtypes")
    protected static IV getValue(final IAccessPath<?> ap, final int index) {

        final IVariableOrConstant<?> t = ap.getPredicate().get(index);

        return (IV) (t == null || t.isVar() ? null : t.get());

    }

}
