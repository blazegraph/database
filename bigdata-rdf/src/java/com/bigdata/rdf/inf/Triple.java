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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.ISPOFilter;

/**
 * A triple pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class Triple extends Pred {
    
    final public ISPOFilter filter;
    
    /**
     * Create a triple/3 predicate.
     * 
     * @param s
     * @param p
     * @param o
     */
    public Triple(VarOrId s, VarOrId p, VarOrId o) {

        this(s, p, o, null/*filter*/);

    }

    /**
     * Create a triple/3 predicate.
     * 
     * @param s
     * @param p
     * @param o
     * @param filter
     *            Optional filter may be used to further restrict the matched
     *            triples.
     */
    public Triple(VarOrId s, VarOrId p, VarOrId o, ISPOFilter filter) {

        super(false, s, p, o);

        this.filter = filter;

    }
    
}

