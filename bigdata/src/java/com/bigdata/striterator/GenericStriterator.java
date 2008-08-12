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
 * Created on Aug 10, 2008
 */

package com.bigdata.striterator;

import java.util.Enumeration;
import java.util.Iterator;

/**
 * Streaming iterator class that supresses generic types. Striterator patterns
 * are often used to convert the type of the elements visited by the underlying
 * iterator. That and the covarying generics combine to make code using generics
 * and striterators rather ugly.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GenericStriterator<E> extends Striterator<Iterator<E>, E> {

    /**
     * @param src
     */
    @SuppressWarnings("unchecked")
    public GenericStriterator(Iterator<E> src) {

        super(src);

    }

    /**
     * @param srcEnum
     */
    @SuppressWarnings("unchecked")
    public GenericStriterator(Enumeration<E> srcEnum) {
    
        super(srcEnum);

    }

}
