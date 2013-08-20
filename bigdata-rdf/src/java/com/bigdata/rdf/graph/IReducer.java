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
/*
 * Created on Jan 16, 2008
 */
package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;

/**
 * An interface for computing reductions over the vertices of a graph.
 * 
 * @param <T>
 *            The type of the result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IResultHandler.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public interface IReducer<VS,ES, ST, T> {

    /**
     * Method is invoked for each result and is responsible for combining the
     * results in whatever manner is meaningful for the procedure.
     * Implementations of this method MUST be <strong>thread-safe</strong>.
     * 
     * @param result
     *            The result from applying the procedure to a single index
     *            partition.
     */
    public void visit(IGASContext<VS, ES, ST> ctx, @SuppressWarnings("rawtypes") IV u);

    /**
     * Return the aggregated results as an implementation dependent object.
     */
    public T get();

}
