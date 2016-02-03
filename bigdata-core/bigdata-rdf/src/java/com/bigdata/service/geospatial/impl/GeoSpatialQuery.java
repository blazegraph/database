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
 * Created on Dec 9, 2015
 */
package com.bigdata.service.geospatial.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.internal.gis.CoordinateUtility;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.IGeoSpatialQuery;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLonTime;

/**
 * Implementation of the {@link IGeoSpatialQuery} interface.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialQuery implements IGeoSpatialQuery {

    private static final Logger log = Logger
            .getLogger(GeoSpatialQuery.class);

    
    // passed in as parameters
    final GeoFunction searchFunction;
    final IConstant<?> subject;
    final TermNode predicate;
    final TermNode context;
    final PointLatLon spatialCircleCenter;
    final Double spatialCircleRadius;
    final PointLatLon spatialRectangleSouthWest;
    final PointLatLon spatialRectangleNorthEast;
    final UNITS spatialUnit;
    final Long timeStart;
    final Long timeEnd;
    final IVariable<?> locationVar;
    final IVariable<?> timeVar;
    final IVariable<?> locationAndTimeVar;
    final IBindingSet incomingBindings;

    // derived properties
    final PointLatLonTime boundingBoxSouthWestWithTime;
    final PointLatLonTime boundingBoxNorthEastWithTime;
    
    /**
     * Constructor
     */
    public GeoSpatialQuery(final GeoFunction searchFunction,
            final IConstant<?> subject, final TermNode predicate,
            final TermNode context, final PointLatLon spatialCircleCenter,
            final Double spatialCircleRadius,
            final PointLatLon spatialRectangleSouthWest,
            final PointLatLon spatialRectangleNorthEast, 
            final UNITS spatialUnit,
            final Long timeStart, final Long timeEnd,
            final IVariable<?> locationVar, final IVariable<?> timeVar,
            final IVariable<?> locationAndTimeVar,
            final IBindingSet incomingBindings) {

        this.searchFunction = searchFunction;
        this.subject = subject;
        this.predicate = predicate;
        this.context = context;
        this.spatialCircleCenter = spatialCircleCenter;
        this.spatialCircleRadius = spatialCircleRadius;
        this.spatialRectangleSouthWest = spatialRectangleSouthWest;
        this.spatialRectangleNorthEast = spatialRectangleNorthEast;
        this.spatialUnit = spatialUnit;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.locationVar = locationVar;
        this.timeVar = timeVar;
        this.locationAndTimeVar = locationAndTimeVar;
        this.incomingBindings = incomingBindings;

        /** 
         * Initialize boundingBoxSouthWestWithTime and boundingBoxNorthEastWithTime
         * with the bounding box for circle queries.
         */
        switch (searchFunction) {

            case IN_CIRCLE:
            {
                
                final CoordinateDD centerPointDD = spatialCircleCenter.asCoordinateDD();
    
                final CoordinateDD southWest = 
                        CoordinateUtility.boundingBoxSouthWest(
                           centerPointDD, spatialCircleRadius, spatialUnit);
                
                final CoordinateDD northEast = 
                        CoordinateUtility.boundingBoxNorthEast(
                           centerPointDD, spatialCircleRadius, spatialUnit);
                
                boundingBoxSouthWestWithTime = new PointLatLonTime(southWest, timeStart);
                boundingBoxNorthEastWithTime = new PointLatLonTime(northEast, timeEnd);
    
                break;
            }

            case IN_RECTANGLE:
            {
                boundingBoxSouthWestWithTime = 
                    new PointLatLonTime(spatialRectangleSouthWest, timeStart);
                
                boundingBoxNorthEastWithTime = 
                    new PointLatLonTime(spatialRectangleNorthEast, timeEnd);
                
                break;
            }
            
            default:
                throw new IllegalArgumentException("Invalid searchFunction: " + searchFunction);
                
        }
    }
    
    /**
     * Private constructor, used for implementing the cloning logics.
     * It is not safe to expose this constructor to the outside: it does
     * not calculate the boundingBoxNorthWestWithTime and 
     * boundingBoxSouthEastWithTime, but expects appropriate values here
     * as input.
     */
    private GeoSpatialQuery(final GeoFunction searchFunction,
            final IConstant<?> subject, final TermNode predicate,
            final TermNode context, final PointLatLon spatialCircleCenter,
            final Double spatialCircleRadius,
            final PointLatLon spatialRectangleSouthWest,
            final PointLatLon spatialRectangleNorthEast, 
            final UNITS spatialUnit,
            final Long timeStart, final Long timeEnd,
            final IVariable<?> locationVar, final IVariable<?> timeVar,
            final IVariable<?> locationAndTimeVar,
            final IBindingSet incomingBindings,
            final PointLatLonTime boundingBoxSouthWestWithTime,
            final PointLatLonTime boundingBoxNorthEastWithTime) {

        this.searchFunction = searchFunction;
        this.subject = subject;
        this.predicate = predicate;
        this.context = context;
        this.spatialCircleCenter = spatialCircleCenter;
        this.spatialCircleRadius = spatialCircleRadius;
        this.spatialRectangleSouthWest = spatialRectangleSouthWest;
        this.spatialRectangleNorthEast = spatialRectangleNorthEast;
        this.spatialUnit = spatialUnit;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.locationVar = locationVar;
        this.timeVar = timeVar;
        this.locationAndTimeVar = locationAndTimeVar;
        this.incomingBindings = incomingBindings;
        
        this.boundingBoxSouthWestWithTime = boundingBoxSouthWestWithTime;
        this.boundingBoxNorthEastWithTime = boundingBoxNorthEastWithTime;
    }

    @Override
    public GeoFunction getSearchFunction() {
        return searchFunction;
    }

    @Override
    public IConstant<?> getSubject() {
        return subject;
    }

    @Override
    public TermNode getPredicate() {
        return predicate;
    }

    @Override
    public TermNode getContext() {
        return context;
    }

    @Override
    public PointLatLon getSpatialCircleCenter() {
        return spatialCircleCenter;
    }

    @Override
    public Double getSpatialCircleRadius() {
        return spatialCircleRadius;
    }

    @Override
    public PointLatLon getSpatialRectangleSouthWest() {
        return spatialRectangleSouthWest;
    }

    @Override
    public PointLatLon getSpatialRectangleNorthEast() {
        return spatialRectangleNorthEast;
    }

    @Override
    public UNITS getSpatialUnit() {
        return spatialUnit;
    }

    @Override
    public Long getTimeStart() {
        return timeStart;
    }

    @Override
    public Long getTimeEnd() {
        return timeEnd;
    }

    @Override
    public IVariable<?> getLocationVar() {
        return locationVar;
    }

    @Override
    public IVariable<?> getTimeVar() {
        return timeVar;
    }

    @Override
    public IVariable<?> getLocationAndTimeVar() {
        return locationAndTimeVar;
    }

    @Override
    public IBindingSet getIncomingBindings() {
        return incomingBindings;
    }

    @Override
    public PointLatLonTime getBoundingBoxSouthWestWithTime() {
        return boundingBoxSouthWestWithTime;
    }
    
    @Override
    public PointLatLonTime getBoundingBoxNorthEastWithTime() {
        return boundingBoxNorthEastWithTime;      
    }
    
    @Override
    public List<IGeoSpatialQuery> normalize() {
        
        if (boundingBoxSouthWestWithTime.getLat()>boundingBoxNorthEastWithTime.getLat()) {
            if (log.isInfoEnabled()) {
               log.info("Search rectangle upper left latitude (" + boundingBoxSouthWestWithTime.getLat() + 
                  ") is larger than rectangle lower righ latitude (" + boundingBoxNorthEastWithTime.getLat() + 
                  ". Search request will give no results.");
            }
            
            return new LinkedList<IGeoSpatialQuery>(); // empty list -> no results
         }
        
         if (boundingBoxSouthWestWithTime.getTimestamp()>boundingBoxNorthEastWithTime.getTimestamp()) {
            if (log.isInfoEnabled()) {
               log.info("Search rectangle upper left timestamp (" + boundingBoxSouthWestWithTime.getTimestamp() + 
                  ") is larger than rectangle lower right timestamp (" + boundingBoxNorthEastWithTime.getTimestamp() + 
                  ". Search request will give no results.");
            }
            
            return new LinkedList<IGeoSpatialQuery>(); // empty list -> no results
         }
         
         /**
          * From here: the query can definitely be normalized
          * -> the next request decides whether normalization is required
          */
         
         
         if (boundingBoxSouthWestWithTime.getLon()>boundingBoxNorthEastWithTime.getLon()) {

            /**
             * This case is actually valid. For instance, we may have a search range from 160 to -160,
             * which we interpret as two search ranges, namely from ]-180;-160] and [160;180].
             */
            if (log.isInfoEnabled()) {
               log.info("Search rectangle upper left latitude (" + boundingBoxSouthWestWithTime.getLat() + 
                  ") is larger than rectangle lower righ latitude (" + boundingBoxNorthEastWithTime.getLat() + 
                  ". Search will be split into two search windows.");
            }

            final List<IGeoSpatialQuery> normalizedQueries = new ArrayList<IGeoSpatialQuery>(2);
            
            final GeoSpatialQuery query1 = 
                cloneWithAdjustedBoundingBox(
                    new PointLatLonTime(
                        boundingBoxSouthWestWithTime.getLat(), 
                        Math.nextAfter(-180.0,0) /** 179.999... */, 
                        boundingBoxSouthWestWithTime.getTimestamp()),
                    new PointLatLonTime(
                        boundingBoxNorthEastWithTime.getLat(), 
                        boundingBoxNorthEastWithTime.getLon(), 
                        boundingBoxNorthEastWithTime.getTimestamp()));
            normalizedQueries.add(query1);
            
            
            final GeoSpatialQuery query2 =
                cloneWithAdjustedBoundingBox(
                    new PointLatLonTime(
                        boundingBoxSouthWestWithTime.getLat(), 
                        boundingBoxSouthWestWithTime.getLon(), 
                        boundingBoxSouthWestWithTime.getTimestamp()),
                    new PointLatLonTime(
                        boundingBoxNorthEastWithTime.getLat(), 
                        180.0, 
                        boundingBoxNorthEastWithTime.getTimestamp()));            
            normalizedQueries.add(query2);

            return normalizedQueries;
            
         } else {

             return Arrays.asList((IGeoSpatialQuery)this);

         }
        
    }

    @Override
    public boolean isNormalized() {

        if (boundingBoxSouthWestWithTime.getLat()>boundingBoxNorthEastWithTime.getLat()) {
           return false; // not normalized (actually unsatisfiable)
        }
        
        if (boundingBoxSouthWestWithTime.getTimestamp()>boundingBoxNorthEastWithTime.getTimestamp()) {
           return false; // not normalized (actually unsatisfiable)
        }
        
        if (boundingBoxSouthWestWithTime.getLon()>boundingBoxNorthEastWithTime.getLon()) {
           return false; // not normalized (but normalizable)
        }
        
        return true; // no violation of normalization detected, all fine

    }

    /**
     * Clones the current query and, as only modification, adjusts the upper left
     * and lower right bounding boxes to the given parameters.
     * 
     * @param boundingBoxSouthWestWithTime
     * @param boundingBoxNorthEastWithTime
     */
    public GeoSpatialQuery cloneWithAdjustedBoundingBox(
        final PointLatLonTime boundingBoxSouthWestWithTime,
        final PointLatLonTime boundingBoxNorthEastWithTime) {
        
        return new GeoSpatialQuery(
            searchFunction, subject, predicate, context, spatialCircleCenter,
            spatialCircleRadius, spatialRectangleSouthWest, spatialRectangleNorthEast, 
            spatialUnit, timeStart, timeEnd, locationVar, timeVar,
            locationAndTimeVar, incomingBindings, boundingBoxSouthWestWithTime,
            boundingBoxNorthEastWithTime);
        
    }

}
