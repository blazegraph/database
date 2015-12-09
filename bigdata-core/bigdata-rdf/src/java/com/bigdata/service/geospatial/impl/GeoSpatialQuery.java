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
 * Interface for geospatial queries and their execution.
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
    final PointLatLon spatialRectangleUpperLeft;
    final PointLatLon spatialRectangleLowerRight;
    final UNITS spatialUnit;
    final Long timeStart;
    final Long timeEnd;
    final IVariable<?> locationVar;
    final IVariable<?> timeVar;
    final IVariable<?> locationAndTimeVar;
    final IBindingSet incomingBindings;

    // derived properties
    final PointLatLonTime boundingBoxUpperLeftWithTime;
    final PointLatLonTime boundingBoxLowerRightWithTime;
    
    /**
     * Constructor
     */
    public GeoSpatialQuery(final GeoFunction searchFunction,
            final IConstant<?> subject, final TermNode predicate,
            final TermNode context, final PointLatLon spatialCircleCenter,
            final Double spatialCircleRadius,
            final PointLatLon spatialRectangleUpperLeft,
            final PointLatLon spatialRectangleLowerRight, 
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
        this.spatialRectangleUpperLeft = spatialRectangleUpperLeft;
        this.spatialRectangleLowerRight = spatialRectangleLowerRight;
        this.spatialUnit = spatialUnit;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.locationVar = locationVar;
        this.timeVar = timeVar;
        this.locationAndTimeVar = locationAndTimeVar;
        this.incomingBindings = incomingBindings;

        /** 
         * Initialize boundingBoxUpperLeftWithTime and boundingBoxLowerRightWithTime
         * with the bounding box for circle queries.
         */
        switch (searchFunction) {

            case IN_CIRCLE:
            {
                
                final CoordinateDD centerPointDD = spatialCircleCenter.asCoordinateDD();
    
                final CoordinateDD upperLeft = 
                        CoordinateUtility.boundingBoxUpperLeft(
                           centerPointDD, spatialCircleRadius, spatialUnit);
                
                final CoordinateDD lowerRight = 
                        CoordinateUtility.boundingBoxLowerRight(
                           centerPointDD, spatialCircleRadius, spatialUnit);
                
                boundingBoxUpperLeftWithTime = new PointLatLonTime(upperLeft, timeStart);
                boundingBoxLowerRightWithTime = new PointLatLonTime(lowerRight, timeEnd);
    
                break;
            }

            case IN_RECTANGLE:
            {
                boundingBoxUpperLeftWithTime = 
                    new PointLatLonTime(spatialRectangleUpperLeft, timeStart);
                
                boundingBoxLowerRightWithTime = 
                    new PointLatLonTime(spatialRectangleLowerRight, timeEnd);
                
                break;
            }
            
            default:
                throw new IllegalArgumentException("Invalid searchFunction: " + searchFunction);
                
        }
    }
    
    /**
     * Private constructor, used for implementing the cloning logics.
     * It is not safe to expose this constructor to the outside: it does
     * not calculate the boundingBoxUpperLeftWithTime and 
     * boundingBoxLowerRightWithTime, but expects appropriate values here
     * as input.
     */
    private GeoSpatialQuery(final GeoFunction searchFunction,
            final IConstant<?> subject, final TermNode predicate,
            final TermNode context, final PointLatLon spatialCircleCenter,
            final Double spatialCircleRadius,
            final PointLatLon spatialRectangleUpperLeft,
            final PointLatLon spatialRectangleLowerRight, 
            final UNITS spatialUnit,
            final Long timeStart, final Long timeEnd,
            final IVariable<?> locationVar, final IVariable<?> timeVar,
            final IVariable<?> locationAndTimeVar,
            final IBindingSet incomingBindings,
            final PointLatLonTime boundingBoxUpperLeftWithTime,
            final PointLatLonTime boundingBoxLowerRightWithTime) {

        this.searchFunction = searchFunction;
        this.subject = subject;
        this.predicate = predicate;
        this.context = context;
        this.spatialCircleCenter = spatialCircleCenter;
        this.spatialCircleRadius = spatialCircleRadius;
        this.spatialRectangleUpperLeft = spatialRectangleUpperLeft;
        this.spatialRectangleLowerRight = spatialRectangleLowerRight;
        this.spatialUnit = spatialUnit;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.locationVar = locationVar;
        this.timeVar = timeVar;
        this.locationAndTimeVar = locationAndTimeVar;
        this.incomingBindings = incomingBindings;
        
        this.boundingBoxUpperLeftWithTime = boundingBoxUpperLeftWithTime;
        this.boundingBoxLowerRightWithTime = boundingBoxLowerRightWithTime;
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
    public PointLatLon getSpatialRectangleUpperLeft() {
        return spatialRectangleUpperLeft;
    }

    @Override
    public PointLatLon getSpatialRectangleLowerRight() {
        return spatialRectangleLowerRight;
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
    public PointLatLonTime getBoundingBoxUpperLeftWithTime() {
        return boundingBoxUpperLeftWithTime;
    }
    
    @Override
    public PointLatLonTime getBoundingBoxLowerRightWithTime() {
        return boundingBoxLowerRightWithTime;      
    }
    
    @Override
    public List<IGeoSpatialQuery> normalize() {
        
        if (boundingBoxUpperLeftWithTime.getLat()>boundingBoxLowerRightWithTime.getLat()) {
            if (log.isInfoEnabled()) {
               log.info("Search rectangle upper left latitude (" + boundingBoxUpperLeftWithTime.getLat() + 
                  ") is larger than rectangle lower righ latitude (" + boundingBoxLowerRightWithTime.getLat() + 
                  ". Search request will give no results.");
            }
            
            return new LinkedList<IGeoSpatialQuery>(); // empty list -> no results
         }
        
         if (boundingBoxUpperLeftWithTime.getTimestamp()>boundingBoxLowerRightWithTime.getTimestamp()) {
            if (log.isInfoEnabled()) {
               log.info("Search rectangle upper left timestamp (" + boundingBoxUpperLeftWithTime.getTimestamp() + 
                  ") is larger than rectangle lower right timestamp (" + boundingBoxLowerRightWithTime.getTimestamp() + 
                  ". Search request will give no results.");
            }
            
            return new LinkedList<IGeoSpatialQuery>(); // empty list -> no results
         }
         
         /**
          * From here: the query can definitely be normalized
          * -> the next request decides whether normalization is required
          */
         
         
         if (boundingBoxUpperLeftWithTime.getLon()>boundingBoxLowerRightWithTime.getLon()) {

            /**
             * This case is actually valid. For instance, we may have a search range from 160 to -160,
             * which we interpret as two search ranges, namely from ]-180;-160] and [160;180].
             */
            if (log.isInfoEnabled()) {
               log.info("Search rectangle upper left latitude (" + boundingBoxUpperLeftWithTime.getLat() + 
                  ") is larger than rectangle lower righ latitude (" + boundingBoxLowerRightWithTime.getLat() + 
                  ". Search will be split into two search windows.");
            }

            final List<IGeoSpatialQuery> normalizedQueries = new ArrayList<IGeoSpatialQuery>(2);
            
            final GeoSpatialQuery query1 = 
                cloneWithAdjustedBoundingBox(
                    new PointLatLonTime(
                        boundingBoxUpperLeftWithTime.getLat(), 
                        Math.nextAfter(-180.0,0) /** 179.999... */, 
                        boundingBoxUpperLeftWithTime.getTimestamp()),
                    new PointLatLonTime(
                        boundingBoxLowerRightWithTime.getLat(), 
                        boundingBoxLowerRightWithTime.getLon(), 
                        boundingBoxLowerRightWithTime.getTimestamp()));
            normalizedQueries.add(query1);
            
            
            final GeoSpatialQuery query2 =
                cloneWithAdjustedBoundingBox(
                    new PointLatLonTime(
                        boundingBoxUpperLeftWithTime.getLat(), 
                        boundingBoxUpperLeftWithTime.getLon(), 
                        boundingBoxUpperLeftWithTime.getTimestamp()),
                    new PointLatLonTime(
                        boundingBoxLowerRightWithTime.getLat(), 
                        180.0, 
                        boundingBoxLowerRightWithTime.getTimestamp()));            
            normalizedQueries.add(query2);

            return normalizedQueries;
            
         } else {

             return Arrays.asList((IGeoSpatialQuery)this);

         }
        
    }

    @Override
    public boolean isNormalized() {

        if (boundingBoxUpperLeftWithTime.getLat()>boundingBoxLowerRightWithTime.getLat()) {
           return false; // not normalized (actually unsatisfiable)
        }
        
        if (boundingBoxUpperLeftWithTime.getTimestamp()>boundingBoxLowerRightWithTime.getTimestamp()) {
           return false; // not normalized (actually unsatisfiable)
        }
        
        if (boundingBoxUpperLeftWithTime.getLon()>boundingBoxLowerRightWithTime.getLon()) {
           return false; // not normalized (but normalizable)
        }
        
        return true; // no violation of normalization detected, all fine

    }

    /**
     * Clones the current query and, as only modification, adjusts the upper left
     * and lower right bounding boxes to the given parameters.
     * 
     * @param boundingBoxUpperLeftWithTime
     * @param boundingBoxLowerRightWithTime
     */
    public GeoSpatialQuery cloneWithAdjustedBoundingBox(
        final PointLatLonTime boundingBoxUpperLeftWithTime,
        final PointLatLonTime boundingBoxLowerRightWithTime) {
        
        return new GeoSpatialQuery(
            searchFunction, subject, predicate, context, spatialCircleCenter,
            spatialCircleRadius, spatialRectangleUpperLeft, spatialRectangleLowerRight, 
            spatialUnit, timeStart, timeEnd, locationVar, timeVar,
            locationAndTimeVar, incomingBindings, boundingBoxUpperLeftWithTime,
            boundingBoxLowerRightWithTime);
        
    }

}