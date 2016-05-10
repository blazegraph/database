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
        set.add(GeoSpatial.SEARCH_DATATYPE);
        set.add(GeoSpatial.PREDICATE);
        set.add(GeoSpatial.CONTEXT);
        set.add(GeoSpatial.SPATIAL_CIRCLE_CENTER);
        set.add(GeoSpatial.SPATIAL_CIRCLE_RADIUS);
        set.add(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST);
        set.add(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST);
        set.add(GeoSpatial.SPATIAL_UNIT);        
        set.add(GeoSpatial.DISTANCE_VALUE);
        set.add(GeoSpatial.TIME_START);
        set.add(GeoSpatial.TIME_END);
        set.add(GeoSpatial.COORD_SYSTEM);
        set.add(GeoSpatial.CUSTOM_FIELDS);
        set.add(GeoSpatial.CUSTOM_FIELDS_LOWER_BOUNDS);
        set.add(GeoSpatial.CUSTOM_FIELDS_UPPER_BOUNDS);        
        set.add(GeoSpatial.LOCATION_VALUE);        
        set.add(GeoSpatial.TIME_VALUE);
        set.add(GeoSpatial.LITERAL_VALUE);
        set.add(GeoSpatial.LAT_VALUE);
        set.add(GeoSpatial.LON_VALUE);
        set.add(GeoSpatial.COORD_SYSTEM_VALUE);
        set.add(GeoSpatial.CUSTOM_FIELDS_VALUES);
        set.add(GeoSpatial.LOCATION_AND_TIME_VALUE);
        
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
