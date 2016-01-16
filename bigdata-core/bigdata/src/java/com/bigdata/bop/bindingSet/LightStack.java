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
 * Created on Sep 3, 2011
 */

package com.bigdata.bop.bindingSet;

import java.util.ArrayList;

/**
 * A light weight stack. Items are stored in the backing {@link ArrayList}. The
 * top of the stack is {@link ArrayList#size()} MINUS ONE (1). The bottom of the
 * stack is index ZERO (0).
 * <p>
 * Note: {@link java.util.Stack} does not offer a constructor which may be used
 * to specify the initial capacity.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements in the stack.
 */
public class LightStack<E> extends ArrayList<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public LightStack(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Push an item onto the stack.
     * 
     * @return That item.
     */
    public E push(final E o) {
        add(o);
        return o;

    }

    /**
     * Remove and return the object at the top of the stack.
     */
    public E pop() {
        return remove(size() - 1);
    }

    /**
     * Return the object on the top of the stack.
     */
    public synchronized E peek() {
        return get(size() - 1);
    }

}
