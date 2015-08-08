/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on July 27, 2015.
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.URI;

import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.service.fts.FTS;
import com.bigdata.service.geospatial.GeoSpatial;

/**
 * Translate {@link FTS#GEOSPATIAL} and related magic predicates into a
 * {@link ServiceNode} which will invoke the bigdata geospatial service.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTGeoSpatialSearchOptimizer extends ASTSearchOptimizerBase {

    static public final Set<URI> searchUris;
   
    static {
      
        final Set<URI> set = new LinkedHashSet<URI>();

        set.add(GeoSpatial.SEARCH);
        set.add(GeoSpatial.PREDICATE);
        set.add(GeoSpatial.SPATIAL_POINT);
        set.add(GeoSpatial.SPATIAL_DISTANCE);
        set.add(GeoSpatial.SPATIAL_DISTANCE_UNIT);
        set.add(GeoSpatial.TIME_POINT);
        set.add(GeoSpatial.TIME_DISTANCE);
        set.add(GeoSpatial.TIME_DISTANCE_UNIT);
        
        searchUris = Collections.unmodifiableSet(set);
    }
   
    @Override
    protected Set<URI> getSearchUris() {
       return searchUris;
    }

    @Override
    protected String getNamespace() {
       return GeoSpatial.NAMESPACE;
    }

    @Override
    protected URI getSearchPredicate() {
       return GeoSpatial.SEARCH;
    }
}
