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

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;


/**
 * <p>
 * A vocabulary for geospatial extension querying, designed to operate over the
 * default index configuration (lat, lon, time).
 * <p>
 * 
 * Example: find all points in the rectangle spanned from (0.00001, 0.00002)
 * to (0.00003, 0.00003) with times between 1-3s.
 * 
 * <pre>
   PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
   SELECT ?res WHERE {
     ?res geo:search "inRectangle" .
     ?res geo:predicate <http://o> .
     ?res geo:spatialRectangleSouthWest "0.00001#0.00002" .
     ?res geo:spatialRectangleNorthEast "0.00003#0.00003" .
     ?res geo:timeStart "1" .
     ?res geo:timeEnd "3" .
   }
 * </pre>
 * 
 * or
 * 
 * Example: find all points in a 0.1 km circle around point (0.00002,0.00002)
 * with times between 1-3s.
 * 
 * <pre>
   PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
   SELECT ?res WHERE {
     ?res geo:search "inCircle" .
     ?res geo:predicate <http://o> .
     ?res geo:spatialCircleCenter "0.00002#0.00002" .
     ?res geo:spatialCircleRadius "0.1" .
     ?res geo:timeStart "1" .
     ?res geo:timeEnd "3" .
   }
 * </pre>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface GeoSpatial {

   public static String CUSTOM_FIELDS_SEPARATOR = "#";
    
   /**
    * Enum for implemented geo functions.
    */
   public static enum GeoFunction {
      IN_CIRCLE("inCircle"),
      IN_RECTANGLE("inRectangle"),
      UNDEFINED("undefined");
      
      GeoFunction(final String name) {
         this.name = name;
      }
      
      public static GeoFunction forName(String name) {
         for (GeoFunction e : GeoFunction.values()) {
             if (e.toString().equalsIgnoreCase(name)) {
                 return e;
             }
         }

         return null;
     }
      
      @Override
      public String toString() {
         return name;
      }
      
      private String name;
   }

   
   public interface Options {
      
      /**
       * Option that may be set to specify a default for {@link GeoSpatial#SEARCH},
       * to be used in geo service. Defaults to {@link GeoFunction#IN_CIRCLE}.
       */
      String GEO_FUNCTION = GeoSpatial.class.getName() + ".defaultGeoFunction";
      
      /**
       * Option that may be set to specify a default for {@link GeoSpatial#SPATIAL_DISTANCE_UNIT},
       * to be used in geo service. Defaults to meters.
       */
      String GEO_SPATIAL_UNIT = GeoSpatial.class.getName() + ".defaultSpatialUnit";
      
      UNITS DEFAULT_GEO_SPATIAL_UNIT = UNITS.Kilometers;


   }

   /************************************************************************************************
    *                                      META DECLARATIONS                                       *
    ************************************************************************************************/

   /**
    * The namespace used for magic search predicates.
    */
   final String NAMESPACE = "http://www.bigdata.com/rdf/geospatial#";


   // do not use anymore -- need to be retained because it is linked by old vocabulary class
   @Deprecated
   final URI DEFAULT_DATATYPE= new URIImpl(NAMESPACE + "geoSpatialLiteral");
   
   
   /**
    * The default datatype to be used for GeoSpatial literals.
    */
   final String GEOSPATIAL_LITERAL_PREFIX_V1  = "http://www.bigdata.com/rdf/geospatial/literals/v1#";
   final String GEOSPATIAL_LITERAL_V1_LAT_LON = GEOSPATIAL_LITERAL_PREFIX_V1 + "lat-lon";
   final String GEOSPATIAL_LITERAL_V1_LAT_LON_TIME = GEOSPATIAL_LITERAL_PREFIX_V1 + "lat-lon-time";
   
   final URI DEFAULT_DATATYPE_LAT_LON = new URIImpl(GEOSPATIAL_LITERAL_V1_LAT_LON);
   final URI DEFAULT_DATATYPE_LAT_LON_TIME = new URIImpl(GEOSPATIAL_LITERAL_V1_LAT_LON_TIME);
   
   
   /************************************************************************************************
    *                                      MAGIC PREDICATES                                        *
    ************************************************************************************************/
   
   /**
    * The name of the search function, pointing to a {@link GeoFunction}.
    */
   final URI SEARCH = new URIImpl(NAMESPACE + "search");

   /**
    * The datatype for literals we're interested in. If not specified, defaults
    * to DEFAULT_DATATYPE.
    */
   final URI SEARCH_DATATYPE = new URIImpl(NAMESPACE + "searchDatatype");

   /**
    * Pointer to the predicate used in scanned triples.
    */
   final URI PREDICATE = new URIImpl(NAMESPACE + "predicate");
   
   /**
    * Pointer to the context used in scanned triples.
    */
   final URI CONTEXT = new URIImpl(NAMESPACE + "context");
   
   /**
    * In case of a {@link GeoFunction#IN_CIRCLE} query only: center point of the bounding circle.
    */
   final URI SPATIAL_CIRCLE_CENTER = new URIImpl(NAMESPACE + "spatialCircleCenter");
   
   /**
    * In case of a {@link GeoFunction#IN_CIRCLE} query only: radius of the bounding circle,
    * specified in SPATIAL_UNIT.
    */   
   final URI SPATIAL_CIRCLE_RADIUS = new URIImpl(NAMESPACE + "spatialCircleRadius");
   
   /**
    * In case of a {@link GeoFunction#IN_RECTANGLE} query only: south west border point of the bounding rectangle.
    */
   final URI SPATIAL_RECTANGLE_SOUTH_WEST = new URIImpl(NAMESPACE + "spatialRectangleSouthWest");
   
   /**
    * In case of a {@link GeoFunction#IN_RECTANGLE} query only: north east border point of the bounding rectangle.
    */   
   final URI SPATIAL_RECTANGLE_NORTH_EAST = new URIImpl(NAMESPACE + "spatialRectangleNorthEast");
   
   /**
    * The spatial unit used for distances specified in the geospatial search request.
    */
   final URI SPATIAL_UNIT = new URIImpl(NAMESPACE + "spatialUnit");

   /**
    * Output variable, supported for "inCircle" query type only. If specified, the variable will, 
    * for each solution, be bound to its distance towards the geospatial circle center.
    */
   final URI DISTANCE_VALUE = new URIImpl(NAMESPACE + "distanceValue");
   
   /**
    * Start time of the time interval to scan for.
    */
   final URI TIME_START = new URIImpl(NAMESPACE + "timeStart");
   
   
   /**
    * End time of the time interval to scan for.
    */
   final URI TIME_END = new URIImpl(NAMESPACE + "timeEnd");

   /**
    * End time of the time interval to scan for.
    */
   final URI COORD_SYSTEM = new URIImpl(NAMESPACE + "coordSystem");
   
   /**
    * #-separated list of custom fields to be queries, e.g. myField1#myField2
    */
   final URI CUSTOM_FIELDS = new URIImpl(NAMESPACE + "customFields");
   
   /**
    * Lower bounds for custom fields, e.g. 10#20
    */
   final URI CUSTOM_FIELDS_LOWER_BOUNDS = new URIImpl(NAMESPACE + "customFieldsLowerBounds");

   /**
    * Upper bounds for custom fields, e.g. 11#5000
    */
   final URI CUSTOM_FIELDS_UPPER_BOUNDS = new URIImpl(NAMESPACE + "customFieldsUpperBounds");

   /**
    * Output variable; if set, this variable is bound to the locations component of the search result.
    */
   final URI LOCATION_VALUE = new URIImpl(NAMESPACE + "locationValue");

   /**
    * Output variable; if set, this variable is bound to the time component of the search result.
    */
   final URI TIME_VALUE = new URIImpl(NAMESPACE + "timeValue");
   
   /**
    * Output variable; if set, this variable is bound to the literal component "as is".
    */
   final URI LITERAL_VALUE = new URIImpl(NAMESPACE + "literalValue");
   
   /**
    * Output variable; if set, this variable is bound to the latitude component of the search result.
    */
   final URI LAT_VALUE = new URIImpl(NAMESPACE + "latValue");

   /**
    * Output variable; if set, this variable is bound to the longitude component of the search result.
    */
   final URI LON_VALUE = new URIImpl(NAMESPACE + "lonValue");

   /**
    * Output variable; if set, this variable is bound to the longitude component of the search result.
    */
   final URI COORD_SYSTEM_VALUE = new URIImpl(NAMESPACE + "coordSystemValue");
   
   /**
    * Output variable; if set, this variable is bound to the values for the custom fields,
    * separated by CUSTOM_FIELDS_SEPARATOR (in case there are multiple custom fields)
    */
   final URI CUSTOM_FIELDS_VALUES = new URIImpl(NAMESPACE + "customFieldsValues");
   
   /**
    * Output variable; if set, this variable is bound to a combined representation of the
    * locations + time component of the search result, separated through the CUSTOM_FIELDS_SEPARATOR.
    */
   final URI LOCATION_AND_TIME_VALUE = new URIImpl(NAMESPACE + "locationAndTimeValue");
   
   
}
