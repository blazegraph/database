/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.spo;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.rule.IAccessPathExpander;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * 
 * @todo Remove reliance on the {@link SPOPredicate}. It only adds the s(), p(),
 *       o(), and c() methods.
 */
public class SPOPredicate extends Predicate<ISPO> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Variable argument version of the shallow copy constructor.
     */
    public SPOPredicate(final BOp[] values, final NV... annotations) {
        super(values, annotations);
    }

    /**
     * Required shallow copy constructor.
     */
    public SPOPredicate(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public SPOPredicate(final SPOPredicate op) {
        super(op);
    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. The
     * predicate is NOT optional. No constraint is specified. No expander is
     * specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, false/* optional */, null/* constraint */, null/* expander */);
        super(
                new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[] { relationName }) //
        );

    }

    /**
     * Partly specified ctor. The predicate is NOT optional. No constraint is
     * specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param c
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final IVariableOrConstant<IV> c) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o, c,
//                false/* optional */, null/* constraint */, null/* expander */);
        super(new IVariableOrConstant[] { s, p, o, c }, //
                new NV(Annotations.RELATION_NAME, relationName) //
        );

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. The
     * predicate is NOT optional. No constraint is specified. No expander is
     * specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     */
    public SPOPredicate(final String[] relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o) {

//        this(relationName, -1/* partitionId */, s, p, o,
//                null/* c */, false/* optional */, null/* constraint */, null/* expander */);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, relationName));

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final boolean optional) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, optional, null/* constraint */, null/* expander */);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[]{relationName}), //
                new NV(Annotations.OPTIONAL, optional));

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final IAccessPathExpander<ISPO> expander) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, false/* optional */, null/* constraint */,
//                expander);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[]{relationName}), //
                new NV(Annotations.ACCESS_PATH_EXPANDER, expander));

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o, final boolean optional,
            final IAccessPathExpander<ISPO> expander) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, optional, null/* constraint */, expander);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[]{relationName}), //
                new NV(Annotations.OPTIONAL, optional), //
                new NV(Annotations.ACCESS_PATH_EXPANDER, expander));
    
    }

    /**
     * Constrain the predicate by setting the context position. If the context
     * position on the {@link SPOPredicate} is non-<code>null</code>, then you
     * must use {@link #asBound(IBindingSet)} to replace all occurrences of the
     * variable appearing in the context position of the predicate with the
     * desired constant. If the context position is already bound the a
     * constant, then you can not modify it (you can only increase the
     * constraint, not change the constraint).
     * 
     * @throws IllegalStateException
     *             unless the context position on the {@link SPOPredicate} is
     *             <code>null</code>.
     * 
     * @deprecated With {@link DefaultGraphSolutionExpander} and
     *             {@link NamedGraphSolutionExpander}.
     */
    @SuppressWarnings("unchecked")
    public SPOPredicate setC(final IConstant<IV> c) {

        if (c == null)
            throw new IllegalArgumentException();

        if (this.get(3) != null)
            throw new IllegalStateException("Context not null: " + c);

        final SPOPredicate tmp = this.clone();

        tmp._set(3/*c*/, c);
        
        return tmp;

    }
    
    public SPOPredicate clone() {

        return (SPOPredicate) super.clone();
        
    }

    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<IV> s() {
        
        return (IVariableOrConstant<IV>) get(0/* s */);
        
    }
    
    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<IV> p() {
        
        return (IVariableOrConstant<IV>) get(1/* p */);
        
    }

    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<IV> o() {
        
        return (IVariableOrConstant<IV>) get(2/* o */);
        
    }
    
    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<IV> c() {
        
        return (IVariableOrConstant<IV>) get(3/* c */);
        
    }

    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SPOPredicate asBound(final IBindingSet bindingSet) {

        return (SPOPredicate) super.asBound(bindingSet);
        
    }

}
