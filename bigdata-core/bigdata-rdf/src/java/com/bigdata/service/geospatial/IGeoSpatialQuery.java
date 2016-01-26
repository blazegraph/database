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
 * Created on July 27, 2015
 */
package com.bigdata.service.geospatial;

import java.util.List;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLonTime;

/**
 * Interface representing (the configuration of) a geospatial query.
 * 
 * See also {@link GeoSpatial} for the vocabulary that can be used
 * to define such a query as SERVICE (or inline).
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IGeoSpatialQuery {

    /**
     * @return the search function underlying the query
     */
    public GeoFunction getSearchFunction();

    /**
     * @return the constant representing the search subject
     */
    public IConstant<?> getSubject();

    /**
     * @return the term node representing the search predicate
     */
    public TermNode getPredicate();

    /**
     * @return the term node representing the search context (named graph)
     */
    public TermNode getContext();

    /**
     * @return the spatial circle center, in case this 
     *          is a {@link GeoFunction#IN_CIRCLE} query
     */
    public PointLatLon getSpatialCircleCenter();

    /**
     * @return the spatial circle radius, in case this 
     *          is a {@link GeoFunction#IN_CIRCLE} query
     */
    public Double getSpatialCircleRadius();

    /**
     * @return the boundary box'es south-west border point.
     */
    public PointLatLon getSpatialRectangleSouthWest();

    /**
     * @return the boundary box'es north-east border point.
     */
    public PointLatLon getSpatialRectangleNorthEast();

    /**
     * @return the spatial unit underlying the query
     */
    public UNITS getSpatialUnit();

    /**
     * @return the start timestamp
     */
    public Long getTimeStart();

    /**
     * @return the end timestamp
     */
    public Long getTimeEnd();

    /**
     * @return the variable to which the location will be bound (if defined)
     */
    public IVariable<?> getLocationVar();

    /**
     * @return the variable to which the time will be bound (if defined)
     */
    public IVariable<?> getTimeVar();

    /**
     * @return the variable to which the location+time will be bound (if defined)
     */
    public IVariable<?> getLocationAndTimeVar();

    /**
     * @return the incoming bindings to join with
     */
    public IBindingSet getIncomingBindings();

    
    /**
     * Get a bounding box including timestamp representing the south-west corner point.
     * 
     * Note that we return south-west since for a south-west pair (lat1, lon1) and
     * its corresponding north-east pair (lat2,lon2) it holds that lat1<=lat2 and lon1<=lon2.
     */
    public PointLatLonTime getBoundingBoxSouthWestWithTime();
    
    /**
     * Get a bounding box including timestamp representing the north-east corner point.
     * 
     * Note that we return north-east since for a north-east pair (lat1, lon1) and
     * its corresponding north-west pair (lat2,lon2) it holds that lat1>=lat2 and lon1>=lon2.
     */
    public PointLatLonTime getBoundingBoxNorthEastWithTime();

    
    /**
     * Normalizes a GeoSpatial query by converting it into an list of GeoSpatial
     * queries that are normalized, see isNormalized(). The list of queries may
     * be empty if the given query contains unsatisfiable range restrictions
     * (e.g., a timestamp or longitude range from [9;8]. However, note that a
     * latitude range from [10;0] will be interpreted as "everything not in the
     * interval ]0;10[.
     */
    public List<IGeoSpatialQuery> normalize();

    /**
     * @return true if the query is normalized. See
     */
    public boolean isNormalized();

}
