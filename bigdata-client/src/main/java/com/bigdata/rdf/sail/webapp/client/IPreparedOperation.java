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
 * Created on Mar 21, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import java.util.UUID;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IPreparedOperation {

   /**
    * Return <code>true</code> iff this is a SPARQL UPDATE request (versus a
    * SPARQL query).
    */
   boolean isUpdate();

   /**
    * Return the {@link UUID} that will be used to uniquely identify this
    * operation (query, update, etc.).
    */
   UUID getQueryId();

    /**
     * Override the value of the specified HTTP header.
     * 
     * @param name
     *            The name of the HTTP header.
     * @param value
     *            The value to be used.
     */
    void setHeader(String name, String value);

    /**
     * Convenience method to set the <code>Accept</code> header.
     * 
     * @param value
     *            The value to be used.
     */
    void setAcceptHeader(String value);
    
    /**
     * Specify the maximum time in milliseconds that the query will be permitted
     * to run. A negative or zero value indicates an unlimited query time (which
     * is the default).
     * 
     * @param millis
     *            The timeout in milliseconds.
     * 
     * @see http://trac.blazegraph.com/ticket/914 (Set timeout on remote query)
     */
    void setMaxQueryMillis(long millis);
 
    /**
     * Return the maximum time in milliseconds that the query will be permitted
     * to run. A negative or zero value indicates an unlimited query time (which
     * is the default).
     * 
     * @return The timeout in milliseceonds.
     */
    long getMaxQueryMillis();
    
    /**
     * Return the value of the specified HTTP header.
     * 
     * @param name
     *            The name of the HTTP header.
     *            
     * @return The value -or- <code>null</code> if the header is not defined.
     */
    String getHeader(String name);
    
}
