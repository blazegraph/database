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
import org.openrdf.model.URI;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.internal.gis.CoordinateUtility;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.service.GeoSpatialConfig;
import com.bigdata.service.GeoSpatialDatatypeConfiguration;
import com.bigdata.service.GeoSpatialDatatypeFieldConfiguration;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatialSearchException;
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
    private final GeoFunction searchFunction;
    private final URI searchDatatype;
    private final IConstant<?> subject;
    private final TermNode predicate;
    private final TermNode context;
    private final PointLatLon spatialCircleCenter;
    private final Double spatialCircleRadius;
    private final PointLatLon spatialRectangleSouthWest;
    private final PointLatLon spatialRectangleNorthEast;
    private final UNITS spatialUnit;
    private final Long timeStart;
    private final Long timeEnd;
    private final IVariable<?> locationVar;
    private final IVariable<?> timeVar;
    private final IVariable<?> locationAndTimeVar;
    private final IBindingSet incomingBindings;

    // derived parameters
    final GeoSpatialDatatypeConfiguration datatypeConfig;
    
    // the (derived) bounding boxes
    CoordinateDD lowerBoundingBox = null;
    CoordinateDD upperBoundingBox = null;
    

    /**
     * Constructor
     */
    public GeoSpatialQuery(final GeoFunction searchFunction,
            final URI searchDatatype,
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
        this.searchDatatype = searchDatatype;
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
        
        this.datatypeConfig = GeoSpatialConfig.getInstance().getConfigurationForDatatype(searchDatatype);

        computeLowerAndUpperBoundingBoxIfNotSet();
    }
    
    /**
     * Private constructor, used for implementing the cloning logics.
     * It is not safe to expose this constructor to the outside: it does
     * not calculate the boundingBoxNorthWestWithTime and 
     * boundingBoxSouthEastWithTime, but expects appropriate values here
     * as input.
     */
    private GeoSpatialQuery(final GeoFunction searchFunction,
            final URI searchDatatype,
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
            final CoordinateDD lowerBoundingBox,
            final CoordinateDD upperBoundingBox) {

        this(searchFunction, searchDatatype, subject, predicate, context, spatialCircleCenter,
             spatialCircleRadius, spatialRectangleSouthWest, spatialRectangleNorthEast,  spatialUnit,
             timeStart, timeEnd, locationVar, timeVar, locationAndTimeVar, incomingBindings);
        
        this.lowerBoundingBox = lowerBoundingBox;
        this.upperBoundingBox = upperBoundingBox;
    }

    @Override
    public GeoFunction getSearchFunction() {
        return searchFunction;
    }

    @Override
    public URI getSearchDatatype() {
        return searchDatatype;
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
    public LowerAndUpperBound getLowerAndUpperBound() {
        
        if (!isNormalized())
            throw new AssertionError("Query must be normalized prior to extracting bounds");
        
        final int numDimensions = datatypeConfig.getNumDimensions();

        final List<GeoSpatialDatatypeFieldConfiguration> fields = datatypeConfig.getFields();

        int latIdx = -1; // no latitude specified
        int lonIdx = -1; // no longitude specified
        final Object[] lowerBound = new Object[numDimensions];
        final Object[] upperBound = new Object[numDimensions];

        /**
         * In the following loop, we initialize everything but latitude and longitude,
         * which require special handling. For the latter, we just remember the indices.
         */
        for (int i=0; i<numDimensions; i++) {
            final GeoSpatialDatatypeFieldConfiguration field = fields.get(i);
            
            switch (field.getServiceMapping()) {
            
            case LATITUDE:
            {
                if (latIdx!=-1) // already set before
                    throw new AssertionError("Multiple latitude mappings for datatype in query.");
                latIdx = i;
                break;
            }  
            case LONGITUDE:
            {
                if (lonIdx!=-1) // already set before
                    throw new AssertionError("Multiple longitude mappings for datatype in query.");
                lonIdx = i;                
                break;
            }
            case TIME:
            {
                if (getTimeStart()==null)
                    throw new GeoSpatialSearchException("Start time not specified in query, but required.");
                if (getTimeEnd()==null)
                    throw new GeoSpatialSearchException("End time not specified in query, but required.");
                lowerBound[i] = getTimeStart();
                upperBound[i] = getTimeEnd();
                break;
            }
            case COORD_SYSTEM:
            // TODO: implement
//            {
//                if (getCoordSystem!=null) {
//                    throw new GeoSpatialSearchException("Coordinate system not specified in query, but required.");                    
//                }
//                lowerBound[i] = getCoordSystem();
//                upperBound[i] = getCoordSystem();                
//            }
            case CUSTOM:
            default:
                // TODO: implement
                throw new IllegalArgumentException("Cases not yet implemented");            
            }
        }
        
        
        
        if (latIdx!=-1 && lonIdx!=-1) {
                
            lowerBound[latIdx] = lowerBoundingBox.northSouth; 
            lowerBound[lonIdx] = lowerBoundingBox.eastWest;

            upperBound[latIdx] = upperBoundingBox.northSouth;
            upperBound[lonIdx] = upperBoundingBox.eastWest;
                
        } else if (latIdx==-1 || lonIdx==-1) {
            
            throw new GeoSpatialSearchException("Latitude and longitude must either both be given or not given.");
            
        } // else: nothing to do, index without classical latitude + longitude components
      
        
        return new LowerAndUpperBound(lowerBound, upperBound);
    }

    
    @Override
    public List<IGeoSpatialQuery> normalize() {
        
        if (!isSatisfiable()) {
            return new ArrayList<IGeoSpatialQuery>();
        }


        /**
         * From here: the query can definitely be normalized
         * -> the next request decides whether normalization is required
         */
        if (lowerBoundingBox.eastWest>upperBoundingBox.eastWest) {

           /**
            * This case is actually valid. For instance, we may have a search range from 160 to -160,
            * which we interpret as two search ranges, namely from ]-180;-160] and [160;180].
            */
           if (log.isInfoEnabled()) {
              log.info("Search rectangle upper left latitude (" + lowerBoundingBox.eastWest + 
                 ") is larger than rectangle lower righ latitude (" + upperBoundingBox.eastWest + 
                 ". Search will be split into two search windows.");
           }

           final List<IGeoSpatialQuery> normalizedQueries = new ArrayList<IGeoSpatialQuery>(2);
            
           final GeoSpatialQuery query1 = 
               new GeoSpatialQuery(
                   searchFunction, searchDatatype, subject, predicate, context, 
                   spatialCircleCenter, spatialCircleRadius, spatialRectangleSouthWest, 
                   spatialRectangleNorthEast, spatialUnit, timeStart, timeEnd, 
                   locationVar, timeVar, locationAndTimeVar, incomingBindings,
                   new CoordinateDD(lowerBoundingBox.northSouth, Math.nextAfter(-180.0,0) /** -179.999... */), 
                   new CoordinateDD(upperBoundingBox.northSouth, upperBoundingBox.eastWest));
            normalizedQueries.add(query1);
            
           final GeoSpatialQuery query2 = 
                   new GeoSpatialQuery(
                       searchFunction, searchDatatype, subject, predicate, context, 
                       spatialCircleCenter, spatialCircleRadius, spatialRectangleSouthWest, 
                       spatialRectangleNorthEast, spatialUnit, timeStart, timeEnd, 
                       locationVar, timeVar, locationAndTimeVar, incomingBindings,
                       new CoordinateDD(lowerBoundingBox.northSouth, lowerBoundingBox.eastWest), 
                       new CoordinateDD(upperBoundingBox.northSouth, 180.0));
           normalizedQueries.add(query2);

            return normalizedQueries;
            
         } else {

             return Arrays.asList((IGeoSpatialQuery)this);

         }
        
    }

    @Override
    public boolean isNormalized() {

        
        if (lowerBoundingBox!=null && upperBoundingBox!=null) {
            
            if ( lowerBoundingBox.eastWest>upperBoundingBox.eastWest) {
                return false; // not normalized (but normalizable)
            }
             
            if (lowerBoundingBox.northSouth>upperBoundingBox.northSouth) {
               return false; // not normalized (actually unsatisfiable)
            }            
        }
        
        return true; // no violation of normalization detected, all fine

    }

    @Override
    public boolean isSatisfiable() {
        
        if (lowerBoundingBox!=null && upperBoundingBox!=null) {
            
            // latitude south west range must be smaller or equal it north east
            if (lowerBoundingBox.northSouth>upperBoundingBox.northSouth) {
                if (log.isInfoEnabled()) {
                   log.info("Search rectangle upper left latitude (" + lowerBoundingBox.northSouth + 
                      ") is larger than rectangle lower righ latitude (" + upperBoundingBox.northSouth + 
                      ". Search request will give no results.");
                }
                
                return false;
             }
         
        }
        
        if (timeStart>timeEnd) {
            
            log.info("Search rectangle start time (" + timeStart + ") is larger than end time (" 
                        + timeEnd +  ". Search request will give no results.");
                
            return false;
        }

        // TODO: add custom mappings here
        
        return true;
    }

    
    /**
     * Set the query's internal bounding box, if required.
     * 
     * TODO: document -> this bounding box doesn't necessarily reflect a normalized query 
     */
    private void computeLowerAndUpperBoundingBoxIfNotSet() {
    
        // if set, use the ones that are set
        if (lowerBoundingBox!=null && upperBoundingBox!=null) {
            return; // already computed
        } // else: continue
    
        final int numDimensions = datatypeConfig.getNumDimensions();
    
        final List<GeoSpatialDatatypeFieldConfiguration> fields = datatypeConfig.getFields();
    
        int latIdx = -1; // no latitude specified
        int lonIdx = -1; // no longitude specified
        for (int i=0; i<numDimensions; i++) {
            final GeoSpatialDatatypeFieldConfiguration field = fields.get(i);
            
            switch (field.getServiceMapping()) {
            
            case LATITUDE:
            {
                if (latIdx!=-1) // already set before
                    throw new AssertionError("Multiple latitude mappings for datatype in query.");
                latIdx = i;
                break;
            }  
            case LONGITUDE:
            {
                if (lonIdx!=-1) // already set before
                    throw new AssertionError("Multiple longitude mappings for datatype in query..");
                lonIdx = i;                
                break;
            }
            default:
                break;
            }
        }
        
        if (latIdx!=-1 && lonIdx!=-1) {
    
            switch (searchFunction) {
    
            case IN_CIRCLE:
            {
                final CoordinateDD centerPointDD = spatialCircleCenter.asCoordinateDD();
    
                lowerBoundingBox = 
                        CoordinateUtility.boundingBoxSouthWest(
                           centerPointDD, spatialCircleRadius, spatialUnit);
    
                upperBoundingBox = 
                        CoordinateUtility.boundingBoxNorthEast(
                           centerPointDD, spatialCircleRadius, spatialUnit);
                break;
            }
            case IN_RECTANGLE:
            {
                lowerBoundingBox = spatialRectangleSouthWest.asCoordinateDD();
                upperBoundingBox =  spatialRectangleNorthEast.asCoordinateDD();
                
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid searchFunction: " + searchFunction);
                
            }
    
        }  // else: no geospatial component
        
    }

}
