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
 * Created on Sep 29, 2010
 */

package com.bigdata.relation.accesspath;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.filter.ITupleFilter;
import com.bigdata.btree.filter.TupleFilter;

import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.IFilterTest;

/**
 * Align the predicate's {@link IElementFilter} constraint with
 * {@link ITupleFilter} so that the {@link IElementFilter} can be evaluated
 * close to the data by an {@link ITupleIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <R>
 *            The generic type of the elements presented to the filter.
 */
public class ElementFilter<R> extends TupleFilter<R> {

    private static final long serialVersionUID = 1L;

    private final IElementFilter<R> test;

    /**
     * Helper method conditionally wraps the <i>test</i>.
     * 
     * @param <R>
     * @param test
     *            The test.
     *            
     * @return The wrapper test -or- <code>null</code> iff the <i>test</i> is
     *         <code>null</code>.
     */
    public static <R> IFilter newInstance(final IElementFilter<R> test) {

        if (test == null)
            return null;
        
        return new ElementFilter<R>(test);
        
    }

    /**
     * Helper method conditionally wraps the <i>test</i>.
     * 
     * @param <R>
     * @param test
     *            The test.
     * 
     * @return The wrapper test -or- <code>null</code> iff the <i>test</i> is
     *         <code>null</code>.
     */
    @SuppressWarnings("rawtypes")
    public static <R> IFilter newInstance(final IFilterTest test) {

        if (test == null)
            return null;
        
        return new ElementFilter(test);
        
    }

    public ElementFilter(final IElementFilter<R> test) {

        if (test == null)
            throw new IllegalArgumentException();

        this.test = test;

    }

    @SuppressWarnings("unchecked")
    public ElementFilter(final IFilterTest test) {

        if (test == null)
            throw new IllegalArgumentException();

        this.test = new FilterTestWrapper(test);

    }
    
    @SuppressWarnings("rawtypes")
    private static class FilterTestWrapper implements IElementFilter {
        private static final long serialVersionUID = 1L;
        private final IFilterTest test;
        private FilterTestWrapper(final IFilterTest test) {
            this.test = test;
        }

        @Override
        public boolean isValid(final Object e) {
            return test.isValid(e);
        }

        @Override
        public boolean canAccept(Object o) {
            return true;
        }

    }

    @Override
    public boolean isValid(final ITuple<R> tuple) {

        final R obj = (R) tuple.getObject();

        return test.isValid(obj);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to show a human readable representation of the test.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{test=" + test);
        sb.append("}");
        return sb.toString();
    }
}
