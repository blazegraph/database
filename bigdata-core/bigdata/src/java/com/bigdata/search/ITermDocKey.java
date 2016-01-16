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
 * Created on Jun 13, 2011
 */

package com.bigdata.search;

/**
 * Interface for the key associated with an entry in the full text index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITermDocKey<V extends Comparable<V>> {

    /**
     * The token text.
     * 
     * @throws UnsupportedOperationException
     *             The sort key for the token text is not decodable. Therefore,
     *             this operation is not supported when reading on the full text
     *             index.
     */
    String getToken() throws UnsupportedOperationException;
    
    /**
     * The normalized local term weight for the token and document in the
     * associated entry of the full text search index.
     */
    double getLocalTermWeight();
    
    /**
     * The document identifier.
     */
    V getDocId();

    /**
     * The field identifier.
     * 
     * @return The field identifier.
     * 
     * @throws UnsupportedOperationException
     *             if the full text index is not storing field identifiers.
     */
    int getFieldId() throws UnsupportedOperationException;
}
