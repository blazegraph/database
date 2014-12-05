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
 * Created on Mar 21, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

/**
 * A prepared boolean query against a {@link JettyRemoteRepository}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPreparedBooleanQuery extends IPreparedQuery {

    /**
     * Evaluate the boolean query.
     * 
     * @param  listener
     *              The query listener.
     * @return The result.
     * 
     * @throws Exception
     */
    boolean evaluate() throws Exception;

    /**
     * Evaluate the boolean query, notify the specified listener when complete.
     * 
     * @param  listener
     *              The query listener.
     * @return The result.
     * 
     * @throws Exception
     */
    boolean evaluate(IPreparedQueryListener listener) throws Exception;
}