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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Appender pattern tacks on another iterator when the source iterator is
 * exhausted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Appender<I extends Iterator<E>,E> implements IFilter<I,E,E> {

    private static final long serialVersionUID = 1307691066685808103L;

    private final I itr2;
    
    public Appender(I itr2) {

        if (itr2 == null)
            throw new IllegalArgumentException();
        
        this.itr2 = itr2;

    }

    @SuppressWarnings("unchecked")
    public I filter(I src) {

        return (I) new AppendingIterator(src, this);

    }

    /**
     * Appending iterator implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <I>
     * @param <E>
     */
    private static class AppendingIterator<I extends Iterator<E>, E> implements
            Iterator<E> {

        /**
         * Initially set to the value supplied to the ctor. When that source is
         * exhausted, this is set to {@link Appender#itr2}. When the second
         * source is exhausted then the total iterator is exhausted.
         */
        private I src;

        private boolean firstSource = true;

        private final Appender<I, E> filter;

        /**
         * @param src
         * @param filter
         */
        public AppendingIterator(I src, Appender<I, E> filter) {

            this.src = src;

            this.filter = filter;

        }

        public boolean hasNext() {

            if (src == null) {

                // exhausted.
                return false;

            }

            if (src.hasNext())
                return true;

            if (firstSource) {

                // start on the 2nd source.
                src = filter.itr2;

            } else {

                // exhausted.
                src = null;

            }

            return hasNext();

        }

        public E next() {

            if (src == null)
                // exhausted.
                throw new NoSuchElementException();

            return src.next();

        }

        public void remove() {

            if (src == null)
                throw new IllegalStateException();

            src.remove();

        }

    }

}
