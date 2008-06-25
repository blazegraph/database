/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.join.rdf;

import com.bigdata.join.IRelationName;

/**
 * This is a implementation that identifies a triple store using the namespace
 * for the triple store.
 * 
 * FIXME How will we identify a TempTripleStore? Note that remote data services
 * can not access a local temporary journal. The temporary store will have to be
 * incorporated into either a transaction (automatically managed life cycle) or
 * an explicitly managed _temporary_ index facility for the federation. I like
 * the idea of using the namespace prefix to identify such indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelationName implements IRelationName {

    private static final long serialVersionUID = -1392983406541791187L;

    private final String name;

    public SPORelationName(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        this.name = name;
        
    }

    public String toString() {
        
        return name;
        
    }
    
}
