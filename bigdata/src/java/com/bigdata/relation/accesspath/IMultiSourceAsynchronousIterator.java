/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Oct 19, 2010
 */

package com.bigdata.relation.accesspath;

/**
 * An interface which permits new sources to be attached dynamically. The
 * decision to accept a new source via {@link #add(IAsynchronousIterator)} or to
 * {@link IMultiSourceAsynchronousIterator#close()} the iterator must be atomic.
 * In particular, it is illegal for a source to be accepted after the iterator
 * has been closed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMultiSourceAsynchronousIterator<E> extends
        IAsynchronousIterator<E> {

    /**
     * Add a source. If the iterator already reports that it is closed then the
     * new source can not be added and this method will return false.
     * 
     * @param src
     *            The source.
     * @return <code>true</code> iff the source could be added.
     */
    boolean add(IAsynchronousIterator<E> src);

}
