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

import com.bigdata.btree.ITupleSerializer;

/**
 * Flyweight implementation.
 * 
 * FIXME smart and compact serialization! Reuse the {@link ITupleSerializer} for
 * the elements and provide compact serialization for the binding sets as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Solution<E> implements ISolution<E> /*, FIXME Serializable*/ {

    private final E e;
    private final IRule rule;
    private final IBindingSet bindingSet;
    
    /**
     * ctor variant when only the element was requested.
     * 
     * @param e
     *            The element.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     */
    public Solution(E e) {
        
        if (e == null)
            throw new IllegalArgumentException();

        this.e = e;
        
        this.rule = null;
        
        this.bindingSet = null;
        
    }

    /**
     * ctor variant when the rule and binding set metadata were requested.
     * 
     * @param e
     *            The element.
     * @param rule
     *            The rule.
     * @param bindingSet
     *            A <strong>copy</strong> of the binding set that will not be
     *            modified by further execution of the rule.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     */
    public Solution(E e, IRule rule, IBindingSet bindingSet) {
        
        if (e == null)
            throw new IllegalArgumentException();
        
        if (rule == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        this.e = e;
        
        this.rule = rule;
        
        this.bindingSet = bindingSet;
        
    }
    
    public E get() {
        
        return e;
        
    }

    public IRule getRule() {

        return rule;
        
    }

    public IBindingSet getBindingSet() {

        return bindingSet;
        
    }

}
