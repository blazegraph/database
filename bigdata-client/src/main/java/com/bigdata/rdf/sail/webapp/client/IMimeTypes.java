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
package com.bigdata.rdf.sail.webapp.client;

/**
 * Common MIME types for dynamic content.
 */
public interface IMimeTypes {

    public String
        MIME_TEXT_PLAIN = "text/plain",
        MIME_TEXT_HTML = "text/html",
//      MIME_TEXT_XML = "text/xml",
        /**
         * General purpose binary <code>application/octet-stream</code>.
         */
        MIME_DEFAULT_BINARY = "application/octet-stream",
		/**
		 * Used for NSS mutation operation responses.
		 */
        MIME_APPLICATION_XML = "application/xml",
	    /**
	     * Used to interchange {@link Properties} objects.
	     */
        MIME_PROPERTIES_XML = "application/xml",
        MIME_TEXT_JAVASCRIPT = "text/javascript",
	    /**
	     * Used to interchange GraphML (blueprints).
	     */
	    MIME_GRAPH_ML = "application/graphml+xml",        	    
        MIME_APPLICATION_JSON = "application/json",
        /**
         * The traditional encoding of URL query parameters within a POST
         * message body.
         */
        MIME_APPLICATION_URL_ENCODED = "application/x-www-form-urlencoded";

}
