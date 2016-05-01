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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.internal.gis.CoordinateUtility;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatialConfig;
import com.bigdata.service.geospatial.GeoSpatialDatatypeConfiguration;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration;
import com.bigdata.service.geospatial.GeoSpatialSearchException;
import com.bigdata.service.geospatial.IGeoSpatialQuery;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;

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
    private final GeoSpatialConfig geoSpatialConfig;
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
    private final Long coordSystem;
    private final Map<String, LowerAndUpperValue> customFieldsConstraints;
    private final IVariable<?> locationVar;
    private final IVariable<?> timeVar;
    private final IVariable<?> locationAndTimeVar;
    private final IVariable<?> latVar;
    private final IVariable<?> lonVar;
    private final IVariable<?> coordSystemVar;
    private final IVariable<?> customFieldsVar;
    private final IVariable<?> literalVar;
    private final IVariable<?> distanceVar;
    private final IBindingSet incomingBindings;

    // derived parameters
    final GeoSpatialDatatypeConfiguration datatypeConfig;
    
    // the (derived) bounding boxes
    CoordinateDD lowerBoundingBox = null;
    CoordinateDD upperBoundingBox = null;
    

    /**
     * Constructor
     */
    public GeoSpatialQuery(final GeoSpatialConfig geoSpatialConfig,
            final GeoFunction searchFunction, final URI searchDatatype,
            final IConstant<?> subject, final TermNode predicate,
            final TermNode context, final PointLatLon spatialCircleCenter,
            final Double spatialCircleRadius,
            final PointLatLon spatialRectangleSouthWest,
            final PointLatLon spatialRectangleNorthEast, 
            final UNITS spatialUnit, final Long timeStart, 
            final Long timeEnd, final Long coordSystem,
            final Map<String, LowerAndUpperValue> customFieldsConstraints,
            final IVariable<?> locationVar, final IVariable<?> timeVar, 
            final IVariable<?> locationAndTimeVar, final IVariable<?> latVar, 
            final IVariable<?> lonVar, final IVariable<?> coordSystemVar, 
            final IVariable<?> customFieldsVar, final IVariable<?> literalVar,
            final IVariable<?> distanceVar, final IBindingSet incomingBindings) {

        this.geoSpatialConfig = geoSpatialConfig;
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
        this.coordSystem = coordSystem;
        this.customFieldsConstraints = customFieldsConstraints;
        this.locationVar = locationVar;
        this.timeVar = timeVar;
        this.locationAndTimeVar = locationAndTimeVar;
        this.latVar = latVar;
        this.lonVar = lonVar;
        this.coordSystemVar = coordSystemVar;
        this.customFieldsVar = customFieldsVar;
        this.incomingBindings = incomingBindings;
        this.literalVar = literalVar;
        this.distanceVar = distanceVar;
        
        this.datatypeConfig = 
            geoSpatialConfig.getConfigurationForDatatype(searchDatatype);
        
        if (this.datatypeConfig==null) {
            throw new GeoSpatialSearchException(
                "Unknown datatype configuration for geospatial search query: " + searchDatatype);
        }
        
        assertConsistency();

        computeLowerAndUpperBoundingBoxIfNotSet();
        
    }
    
    /**
     * Private constructor, used for implementing the cloning logics.
     * It is not safe to expose this constructor to the outside: it does
     * not calculate the boundingBoxNorthWestWithTime and 
     * boundingBoxSouthEastWithTime, but expects appropriate values here
     * as input.
     */
    private GeoSpatialQuery(final GeoSpatialConfig geoSpatialConfig,
            final GeoFunction searchFunction,
            final URI searchDatatype,
            final IConstant<?> subject, final TermNode predicate,
            final TermNode context, final PointLatLon spatialCircleCenter,
            final Double spatialCircleRadius,
            final PointLatLon spatialRectangleSouthWest,
            final PointLatLon spatialRectangleNorthEast, 
            final UNITS spatialUnit,
            final Long timeStart, final Long timeEnd,
            final Long coordSystem, 
            final Map<String, LowerAndUpperValue> customFieldsConstraints,
            final IVariable<?> locationVar, 
            final IVariable<?> timeVar,
            final IVariable<?> locationAndTimeVar,
            final IVariable<?> latVar, 
            final IVariable<?> lonVar, 
            final IVariable<?> coordSystemVar, 
            final IVariable<?> customFieldsVar,
            final IVariable<?> literalVar,
            final IVariable<?> distanceVar,
            final IBindingSet incomingBindings,
            final CoordinateDD lowerBoundingBox,
            final CoordinateDD upperBoundingBox) {

        this(geoSpatialConfig, searchFunction, searchDatatype, subject, predicate, context, spatialCircleCenter,
             spatialCircleRadius, spatialRectangleSouthWest, spatialRectangleNorthEast,  spatialUnit,
             timeStart, timeEnd, coordSystem, customFieldsConstraints, locationVar, timeVar, locationAndTimeVar, 
             latVar, lonVar, coordSystemVar, customFieldsVar, literalVar, distanceVar, incomingBindings);
        
        this.lowerBoundingBox = lowerBoundingBox;
        this.upperBoundingBox = upperBoundingBox;
    }
    
    
    /**
     * Constructs a validated custom fields constraints from the parsed user input. Throws
     * an exception if the input arity does not match or is invalid (i.e., a lower bound is 
     * specified to be larger than an upper bound).
     * 
     * @param customFields the custom field definitions
     * @param customFieldsLowerBounds the lower bounds for the custom fields definition (needs to have same arity)
     * @param customFieldsUpperBounds the upper bounds for the custom fields definition (needs to have same arity)
     * @return
     */
    public static Map<String, LowerAndUpperValue> toValidatedCustomFieldsConstraints(
        final String[] customFields, final Object[] customFieldsLowerBounds, final Object[] customFieldsUpperBounds) {
        
        final  Map<String, LowerAndUpperValue> customFieldsConstraints = new HashMap<String, LowerAndUpperValue>();
        
        if (customFields.length!=customFieldsLowerBounds.length)
            throw new GeoSpatialSearchException(
                "Nr of custom fields = " + customFields.length + 
                " differs from number of lower bounds = " + customFieldsLowerBounds.length);
        
        if (customFields.length!=customFieldsUpperBounds.length)
            throw new GeoSpatialSearchException(
                "Nr of custom fields = " + customFields.length + 
                " differs from number of upper bounds = " + customFieldsUpperBounds.length);

        for (int i=0; i<customFields.length; i++) {
          
            customFieldsConstraints.put(
                customFields[i], 
                new LowerAndUpperValue(customFieldsLowerBounds[i],customFieldsUpperBounds[i]));
        }
        
        return customFieldsConstraints;
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
    public Long getCoordSystem() {
        return coordSystem;
    }

    @Override
    public Map<String, LowerAndUpperValue> getCustomFieldsConstraints() {
        return customFieldsConstraints;
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
    public IVariable<?> getLatVar() {
        return latVar;
    }

    @Override
    public IVariable<?> getLonVar() {
        return lonVar;
    }

    @Override
    public IVariable<?> getCoordSystemVar() {
        return coordSystemVar;
    }

    @Override
    public IVariable<?> getCustomFieldsVar() {
        return customFieldsVar;
    }
    
    @Override
    public IVariable<?> getLocationAndTimeVar() {
        return locationAndTimeVar;
    }
    
    
    @Override
    public IVariable<?> getLiteralVar() {
        return literalVar;
    }
    
    @Override
    public IVariable<?> getDistanceVar() {
        return distanceVar;
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
            {
                if (getCoordSystem()==null) {
                    throw new GeoSpatialSearchException("Coordinate system not specified in query, but required.");                    
                }
                lowerBound[i] = getCoordSystem();
                upperBound[i] = getCoordSystem();
                break;
            }
            case CUSTOM:
            {
                final String customServiceMapping = field.getCustomServiceMapping();
                if (!customFieldsConstraints.containsKey(customServiceMapping)) {
                    throw new GeoSpatialSearchException(
                        "Custom field " + customServiceMapping + " not specified in query, but required.");
                }
                
                final LowerAndUpperValue v = customFieldsConstraints.get(customServiceMapping);
                lowerBound[i] = v.lowerValue;
                upperBound[i] = v.upperValue;
                
                break;
            }
            default:
                throw new IllegalArgumentException("Cases not implemented");            
            }
        }
        
        
        
        if (latIdx!=-1 && lonIdx!=-1) {
                
            lowerBound[latIdx] = lowerBoundingBox.northSouth; 
            lowerBound[lonIdx] = lowerBoundingBox.eastWest;

            upperBound[latIdx] = upperBoundingBox.northSouth;
            upperBound[lonIdx] = upperBoundingBox.eastWest;
                
        } else if (latIdx==-1 && lonIdx==-1) {
            
             // nothing to be done: no geospatial cooordinates used in z-order index
            
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
        if (lowerBoundingBox!=null && upperBoundingBox!=null && lowerBoundingBox.eastWest>upperBoundingBox.eastWest) {

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
                   geoSpatialConfig, searchFunction, searchDatatype, subject, predicate, context, 
                   spatialCircleCenter, spatialCircleRadius, spatialRectangleSouthWest, 
                   spatialRectangleNorthEast, spatialUnit, timeStart, timeEnd, coordSystem,
                   customFieldsConstraints, locationVar, timeVar, locationAndTimeVar, latVar, 
                   lonVar, coordSystemVar, customFieldsVar, literalVar, distanceVar, incomingBindings,
                   new CoordinateDD(lowerBoundingBox.northSouth, Math.nextAfter(-180.0,0) /** -179.999... */), 
                   new CoordinateDD(upperBoundingBox.northSouth, upperBoundingBox.eastWest));
            normalizedQueries.add(query1);
            
            final GeoSpatialQuery query2 = 
                new GeoSpatialQuery(
                    geoSpatialConfig, searchFunction, searchDatatype, subject, predicate, context, 
                    spatialCircleCenter, spatialCircleRadius, spatialRectangleSouthWest, 
                    spatialRectangleNorthEast, spatialUnit, timeStart, timeEnd, coordSystem,
                    customFieldsConstraints, locationVar, timeVar, locationAndTimeVar, latVar, 
                    lonVar, coordSystemVar, customFieldsVar, literalVar, distanceVar, incomingBindings, 
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
            
            // latitude south west range must be smaller or equal than north east
            if (lowerBoundingBox.northSouth>upperBoundingBox.northSouth) {
                
                return false;
             }
         
        }
        
        if (timeStart!=null && timeEnd!=null) {
            if (timeStart>timeEnd) {
                    
                return false;
            }
        }
        
        final Map<String, LowerAndUpperValue> cfcs = getCustomFieldsConstraints();
        for (final String key : cfcs.keySet()) {
            final LowerAndUpperValue val = cfcs.get(key);
            
            if (gt(val.lowerValue,val.upperValue)) {
                
                return false;
            }
        }
        
        return true;
    }

    void assertConsistency() {

        // simple existence checks for required properties
        if (predicate==null) {
            throw new GeoSpatialSearchException(GeoSpatial.PREDICATE + " must be bound but is null.");
        }
            
        if (searchDatatype==null) {
            throw new GeoSpatialSearchException(GeoSpatial.SEARCH_DATATYPE + " must be bound but is null.");
        }

        // lookup of geospatial component in non-geospatial (custom) z-order index
        if (locationVar!=null || locationAndTimeVar!=null || latVar!=null || lonVar!=null || distanceVar!=null) {
            if (!(datatypeConfig.hasLat() && datatypeConfig.hasLon())) {
                throw new GeoSpatialSearchException(
                    "Requested extraction of geospatial coordinates (via " + GeoSpatial.LOCATION_AND_TIME_VALUE + ", "
                    + GeoSpatial.LOCATION_AND_TIME_VALUE + ", " + GeoSpatial.LAT_VALUE + ", " + GeoSpatial.LON_VALUE + 
                    " or " + GeoSpatial.DISTANCE_VALUE + ")" + " but the index contains no geospatial coordinates. " + 
                    "Please remove the respective predicated from your query.");
            }
        }
        
        // lookup of time component in index not containing time
        if (timeVar!=null && !datatypeConfig.hasTime()) {
            throw new GeoSpatialSearchException(
                "Requested extraction of time via " + GeoSpatial.TIME_VALUE 
                + " but index does not contain time component.");
        }
        
        // lookup of time component via locatioAndTime in index not containing time
        if (locationAndTimeVar!=null && !datatypeConfig.hasTime()) {
            throw new GeoSpatialSearchException(
                "Requested extraction of time via " + GeoSpatial.LOCATION_AND_TIME_VALUE 
                + " but index does not contain time component.");
        }
        
        // lookup of coordinate system in index not containing coordinate system identifier
        if (coordSystemVar!=null && !datatypeConfig.hasCoordSystem()) {
            throw new GeoSpatialSearchException(
                "Requested extraction of coordinate system via " + GeoSpatial.COORD_SYSTEM_VALUE 
                + " but index does not contain coordinate system component.");
        }
        
        // lookup of custom fields where no custom fields are defined
        if (customFieldsVar!=null && !datatypeConfig.hasCustomFields()) {
            throw new GeoSpatialSearchException(
                    "Requested extraction of custom fields via " + GeoSpatial.CUSTOM_FIELDS_VALUES 
                    + " but index does not define any custom fields.");
        }

        // assert that the custom fields defined in the index are identical with the specified custom fields
        final Set<String> datatypeCustomFields = datatypeConfig.getCustomFieldsIdxs().keySet();
        
        if ((!getCustomFieldsConstraints().keySet().containsAll(datatypeCustomFields)) || 
            (!datatypeCustomFields.containsAll(getCustomFieldsConstraints().keySet()))) {
            
            throw new GeoSpatialSearchException(
                    "The custom fields defined in the datatype (" + Arrays.toString(datatypeCustomFields.toArray())
                    + ") differs from the custom fields defined in the query (" 
                    + Arrays.toString(getCustomFieldsConstraints().keySet().toArray()) + "). "
                    + "You need to specify the upper and lower bounds for all custom components of "
                    + "the index using predicates " + GeoSpatial.CUSTOM_FIELDS + ", " + GeoSpatial.CUSTOM_FIELDS_LOWER_BOUNDS
                    + ", and " + GeoSpatial.CUSTOM_FIELDS_UPPER_BOUNDS + ".");
        }

        switch (searchFunction) 
        {
        case IN_CIRCLE:
        {
            if (!(datatypeConfig.hasLat() && datatypeConfig.hasLon())) {
                throw new GeoSpatialSearchException(
                    "Search function inCircle used for datatype having no geospatial components.");
            }
                
            if (spatialCircleCenter==null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_CIRCLE_CENTER + " not supported for search function inCircle.");
            }
            
            if (spatialCircleRadius==null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_CIRCLE_RADIUS + " not supported for search function inCircle.");                
            }
            
            if (spatialRectangleSouthWest!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST + " not supported for search function inCircle.");                                
            }
            
            if (spatialRectangleNorthEast!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST + " not supported for search function inCircle.");                                                
            }
                
            break;
        }
        case IN_RECTANGLE:
        {
            if (!(datatypeConfig.hasLat() && datatypeConfig.hasLon())) {
                throw new GeoSpatialSearchException(
                    "Search function inRectangle used for datatype having no geospatial components.");
            }
            
            if (spatialRectangleSouthWest==null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST + " not supported for search function inRectangle.");                                
            }
            
            if (spatialRectangleNorthEast==null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST + " not supported for search function inRectangle.");                                                
            }
            
            if (spatialCircleCenter!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_CIRCLE_CENTER + " not supported for search function inRectangle.");
            }
            
            if (spatialCircleRadius!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_CIRCLE_RADIUS + " not supported for search function inRectangle.");                
            }
            
            if (distanceVar!=null) {
                throw new GeoSpatialSearchException(
                        "Predicate " + GeoSpatial.DISTANCE_VALUE + " not supported for search function inRectangle.");                                
            }
            
            break;
        }
        case UNDEFINED:
        {
            if (datatypeConfig.hasLat() || datatypeConfig.hasLon()) {
                throw new GeoSpatialSearchException(
                    "No search function given, but required since datatype has geospatial components.");
            }
            
            if (spatialCircleCenter!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_CIRCLE_CENTER + " must not be provided "
                     + "for query against index without geospatial components.");
            }
            
            if (spatialCircleRadius!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_CIRCLE_RADIUS + " must not be provided "
                    + "for query against index without geospatial components.");                
            }
            
            if (spatialRectangleSouthWest!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST + " must not be provided "
                    + "for query against index without geospatial components.");
            }
            
            if (spatialRectangleNorthEast!=null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST + " must not be provided "
                    + "for query against index without geospatial components.");
            }
            
            break;
        }
        default:
            throw new GeoSpatialSearchException("Unhandled search function: " + searchFunction);
        }
        
        
        // datatype has time but time not given in query
        if (datatypeConfig.hasTime()) {
            if (timeStart==null || timeEnd==null) {
                throw new GeoSpatialSearchException(
                    "Predicate " + GeoSpatial.TIME_START + " and " + GeoSpatial.TIME_END 
                    + " must be provided when querying index with time component");
            }
        }
        
        // query has time but unusable (since not present in datatype)
        if (timeStart!=null || timeEnd!=null) {
            if (!datatypeConfig.hasTime()) {
                throw new GeoSpatialSearchException(
                     "Predicate " + GeoSpatial.TIME_START + " or " + GeoSpatial.TIME_END 
                     + " specified in query, but datatype that is queried does not have a time component.");                
            }
        }

        // datatype has coord system but coord system not given in query
        if (datatypeConfig.hasCoordSystem() && coordSystem==null) {
            throw new GeoSpatialSearchException(
                "Predicate " + GeoSpatial.COORD_SYSTEM + 
                " must be provided when querying index with coordinate system component");
        }
        
        // coord system in query but unusable (since not present in datatype)
        if (coordSystem!=null && !datatypeConfig.hasCoordSystem()) {
             throw new GeoSpatialSearchException(
                  "Predicate " + GeoSpatial.COORD_SYSTEM + " specified in query, "
                  + "but datatype that is queried does not have a coordinate system component.");                
        }

    }
    
    @Override
    public GeoSpatialDatatypeConfiguration getDatatypeConfig() {
        return datatypeConfig;
    }
    
    /**
     * Set the query's internal bounding box, if required. The bounding box
     * that we compute does not necessarily represent a valid query, i.e.
     * the query in the general case requires normalization in order to
     * give valid z-order search strings later on.
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
                throw new IllegalArgumentException("Search function (geo:search) must be defined.");
                
            }
    
        }  // else: no geospatial component
        
    }
    
    /**
     * Compare two values of either type Double or Long using greater-than
     * Throws an {@link GeoSpatialSearchException} if not both are of type Double
     * or if they are not both of type Long.
     */
    private static boolean gt(final Object lower, final Object upper) {
        
        if ((lower instanceof Double) && (upper instanceof Double)) {

            return (Double)lower>(Double)upper;
            
        } else if ((lower instanceof Long) && (upper instanceof Long)) {

            return (Long)lower>(Long)upper;
            
        } else {

            throw new GeoSpatialSearchException(
                "Incompatible types for lower and upper bound. Something's wrong in the implementation.");
            
        }
    }

}
