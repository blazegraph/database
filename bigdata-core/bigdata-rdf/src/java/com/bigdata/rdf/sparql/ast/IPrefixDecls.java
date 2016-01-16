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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

/**
 * Interface for prefix declarations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPrefixDecls {

    public interface Annotations {

        /**
         * The namespace prefix declaration map. This is a {@link Map} with
         * {@link String} keys (prefix) and {@link String} values (the uri
         * associated with that prefix).
         */
        String PREFIX_DECLS = "prefixDecls";

    }
    
    /**
     * The namespace prefix declarations map. This is a {@link Map} with
     * {@link String} keys (prefix) and {@link String} values (the uri
     * associated with that prefix).
     * 
     * @return The namespace prefix declarations map. If this annotation was not
     *         set, then an empty map will be returned. The returned map is
     *         immutable to preserve the general contract for notification on
     *         mutation.
     */
    public Map<String, String> getPrefixDecls();

    /**
     * Set the namespace prefix declarations map. This is a {@link Map} with
     * {@link String} keys (prefix) and {@link String} values (the uri
     * associated with that prefix).
     */
    void setPrefixDecls(final Map<String, String> prefixDecls);
    
}
