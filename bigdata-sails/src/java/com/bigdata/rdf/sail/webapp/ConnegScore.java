/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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

    // /**
    // * The as given MIME type.
    // */
    // final public String mimeType;

    public ConnegScore(final float q, final E format) {
        // , final String mimeTypeIsIgnored) {

        if (format == null)
            throw new IllegalArgumentException();

        // if (mimeType == null)
        // throw new IllegalArgumentException();

        this.q = q;

        this.format = format;

        // this.mimeType = mimeType;

    }

    @Override
    public int compareTo(final ConnegScore<E> o) {

        if (q < o.q)
            return -1;

        if (q > o.q)
            return 1;

        return 0;

    }

    public String toString() {

        return getClass().getSimpleName() + "{q=" + q + ", format=" + format
                + "}";

    }

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