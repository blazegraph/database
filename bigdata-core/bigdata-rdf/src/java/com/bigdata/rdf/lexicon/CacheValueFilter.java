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
 * Created on Sep 28, 2010
 */

package com.bigdata.rdf.lexicon;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.ap.filter.BOpResolver;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Cache the {@link BigdataValue} on the {@link IV} (create a cross linkage).
 * This is useful for lexicon joins and SPARQL operators that need to use
 * materialized RDF values.
 */
public class CacheValueFilter extends BOpResolver {

    /**
	 * 
	 */
	private static final long serialVersionUID = -7267351719878117114L;

	/** 
     * A default instance.
     */
    public static CacheValueFilter newInstance() {
        return new CacheValueFilter(BOp.NOARGS, BOp.NOANNS);
    }

    /**
     * @param op
     */
    public CacheValueFilter(CacheValueFilter op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public CacheValueFilter(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Cache the BigdataValue on its IV (cross-link).
     */
    @Override
    protected Object resolve(final Object obj) {

		final BigdataValue val = (BigdataValue) obj;
		
		// the link from BigdataValue to IV is pre-existing (set by the
		// materialization of the index tuple)
		final IV iv = val.getIV();
    	
    	// cache the value on the IV
    	iv.setValue(val);
		
		return obj;

    }

}
