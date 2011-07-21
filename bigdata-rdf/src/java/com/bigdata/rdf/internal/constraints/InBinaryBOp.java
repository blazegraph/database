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
package com.bigdata.rdf.internal.constraints;

import java.util.Arrays;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;

/**
 * A constraint that a variable may only take on the bindings enumerated by some
 * set.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: INBinarySearch.java 4357 2011-03-31 17:11:26Z thompsonbry $
 */
public class InBinaryBOp extends InBOp {

	private static final long serialVersionUID = 2251370041131847351L;

	/**
     * The variable (cached).
     * <p>
     * Note: This cache is not serialized and is compiled on demand when the
     * operator is used.
     */
    private transient volatile IVariable<IV> var;

    /**
     * The sorted data (cached).
     * <p>
     * Note: This cache is not serialized and is compiled on demand when the
     * operator is used.
     */
    private transient volatile IV[] set;
    
    private transient boolean not;

    /**
     * Deep copy constructor.
     */
    public InBinaryBOp(final InBinaryBOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     */
    public InBinaryBOp(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     *
     * @param x
     *            Some variable.
     * @param set
     *            A set of legal term identifiers providing a constraint on the
     *            allowable values for that variable.
     */
    public InBinaryBOp(boolean not,final IVariable var, final IConstant[] set) {
        super(not,var,set);
    }

    @SuppressWarnings("unchecked")
    static private  IV[] sort(final IConstant<IV>[] set) {

        final int n = set.length;

        if (n == 0)
            throw new IllegalArgumentException();

        final IV firstValue = set[0].get();

        // allocate an array of the correct type.
        final IV[] tmp = (IV[]) java.lang.reflect.Array.newInstance(firstValue
                .getClass(), n);

        for (int i = 0; i < n; i++) {

            // dereference the constants to their bound values.
            tmp[i] = set[i].get();

        }

        // sort the bound values.
        Arrays.sort(tmp);

        return tmp;

    }

    private void init() {

        var = getVariable();

        set = sort(getSet());
        not=((Boolean)getProperty(Annotations.NOT)).booleanValue();
    }

    public boolean accept(final IBindingSet bindingSet) {
        if (var == null) {
            synchronized (this) {
                if (var == null) {
                    // init() is guarded by double-checked locking pattern.
                    init();
                }
            }
        }

        // get binding for "x".
        @SuppressWarnings("unchecked")
        final IConstant<IV> x = bindingSet.get(var);

        if (x == null) {
            throw new SparqlTypeErrorException.UnboundVarException();
        }

        final IV v = x.get();

        // lookup the bound value in the set of values.
        final int pos = Arrays.binarySearch(set, v);

        // true iff the bound value was found in the set.
        final boolean found = pos >= 0;

        return not?!found:found;

    }

}
