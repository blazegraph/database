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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;

/**
 * Helper class used to rank content types based on their quality scores.
 * 
 * @param <E>
 */
public class ConnegScore<E> implements Comparable<ConnegScore<E>> {

    /**
     * The quality score.
     */
    final public float q;

    /**
     * The type of format.
     */
    final public E format;

    public ConnegScore(final float q, final E format) {

        if (format == null)
            throw new IllegalArgumentException();

        this.q = q;

        this.format = format;

    }

    /**
     * The higher the <code>q</code> score the better the match with the user
     * agent's preference. A mime type without an explicit <code>q</code> score
     * has an implicit score of <code>1</code>.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/920" > Content negotiation
     *      orders accept header scores in reverse </a>
     */
    @Override
    public int compareTo(final ConnegScore<E> o) {

        if (q < o.q)
            return 1;

        if (q > o.q)
            return -1;

        return 0;

    }

    @Override
    public String toString() {

        return getClass().getSimpleName() + "{q=" + q + ", format=" + format
                + "}";

    }

    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof ConnegScore))
            return false;

        final ConnegScore<?> t = (ConnegScore<?>) o;

        if (this.format != t.format)
            return false;

        if (this.q != t.q)
            return false;

        return true;

    }

}
