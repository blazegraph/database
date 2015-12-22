/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jun 17, 2008
 */

package com.bigdata.bop.constraint;

import java.util.Arrays;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;

/**
 * A constraint that a variable may only take on the bindings enumerated by some
 * set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class INBinarySearch<T> extends INConstraint<T> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2251370041131847351L;

	/**
     * The variable (cached).
     * <p>
     * Note: This cache is not serialized and is compiled on demand when the
     * operator is used.
     */
    private transient volatile IVariable<T> var;
    
    /**
     * The sorted data (cached).
     * <p>
     * Note: This cache is not serialized and is compiled on demand when the
     * operator is used.
     */
    private transient volatile T[] set;
    
    /**
     * Deep copy constructor.
     */
    public INBinarySearch(final INBinarySearch<T> op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     */
    public INBinarySearch(final BOp[] args, final Map<String, Object> annotations) {
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
    public INBinarySearch(final IVariable<T> x, final IConstant<T>[] set) {

        super(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Annotations.VARIABLE, x),//
                new NV(Annotations.SET, set),//
                }));
        
    }

    @SuppressWarnings("unchecked")
    static private <T> T[] sort(final IConstant<T>[] set) {

        final int n = set.length;
        
        if (n == 0)
            throw new IllegalArgumentException();
        
        final T firstValue = set[0].get();
        
        // allocate an array of the correct type.
        final T[] tmp = (T[]) java.lang.reflect.Array.newInstance(firstValue
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

    }
    
    public Boolean get(final IBindingSet bindingSet) {
        
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
        final IConstant<T> x = bindingSet.get(var);

        if (x == null) {

            // not yet bound : FIXME Modify to return false - variables must be bound
            return true;
            
        }

        final T v = x.get();

        // lookup the bound value in the set of values.
        final int pos = Arrays.binarySearch(set, v);

        // true iff the bound value was found in the set.
        final boolean found = pos >= 0;

        return found;

    }

}
