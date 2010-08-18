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
import java.util.HashSet;

import com.bigdata.bop.AbstractBOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpList;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.spo.InGraphBinarySearchFilter;
import com.bigdata.rdf.spo.InGraphHashSetFilter;

/**
 * A constraint that a variable may only take on the bindings enumerated by some
 * set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This uses binary search, which is thread-safe. It could also use a
 *       {@link HashSet}, but the {@link HashSet} needs to be thread-safe since
 *       the filter could be applied concurrently during evaluation.
 * 
 *       FIXME Reconcile this with {@link InGraphBinarySearchFilter} and
 *       {@link InGraphHashSetFilter} and also with the use of an in-memory join
 *       against the incoming binding sets to handle SPARQL data sets.
 */
public class IN<T> extends AbstractBOp implements IConstraint {

//    /**
//     * 
//     */
//    private static final long serialVersionUID = 5805883429399100605L;
//
//    private final IVariable<T> x;
//    
//    private final T[] set;

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The sorted data (cached).
     * <p>
     * Note: This cache is redundant with the 2nd argument to the operator. It
     * is not serialized and is compiled on demand when the operator is used.
     */
    private transient volatile T[] set;
    
    /**
     * 
     * @param x
     *            Some variable.
     * @param set
     *            A set of legal term identifiers providing a constraint on the
     *            allowable values for that variable.
     */
    public IN(final IVariable<T> x, final IConstant<T>[] set) {

        super(new BOp[] { x, new BOpList(set) });
        
        if (x == null || set == null)
            throw new IllegalArgumentException();

        if (set.length == 0)
            throw new IllegalArgumentException();
        
    }

    @SuppressWarnings("unchecked")
    static private <T> T[] sort(final BOpList set) {

        final int n = set.arity();
        
        if (n == 0)
            throw new IllegalArgumentException();
        
        final T firstValue = ((IConstant<T>) set.get(0)).get();
        
        // allocate an array of the correct type.
        final T[] tmp = (T[]) java.lang.reflect.Array.newInstance(firstValue
                .getClass(), n);

        for (int i = 0; i < n; i++) {

            // dereference the constants to their bound values.
            tmp[i] = ((IConstant<T>) set.get(i)).get();
            
        }
        
        // sort the bound values.
        Arrays.sort(tmp);
        
        return tmp;

    }
    
    public boolean accept(final IBindingSet bindingSet) {
        
        if(set == null) {

            set = sort((BOpList) args[1]);
            
        }
        
        // get binding for "x".
        @SuppressWarnings("unchecked")
        final IConstant<T> x = bindingSet.get((IVariable<?>) args[0]/* x */);

        if (x == null) {

            // not yet bound.
            return true;
            
        }

        final T v = x.get();

        // lookup the bound value in the set of values.
        final int pos = Arrays.binarySearch(set, v);
        
        // true iff the bound value was found in the set.
        return pos >= 0;

    }

}
