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
 * Created on July 27, 2015
 */
package com.bigdata.service.geospatial;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;


/**
 * <p>
 * A vocabulary for geospatial extension querying.
 * <p>
 * Example:
 * 
 * <pre>
 * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
 * SELECT ?res WHERE {
 *   ?res geo:search "inCircle" .
 *   ?res geo:spatialPoint "12.1#12.4" .
 *   ?res geo:spatialDistance "0.01" . # radius
 *   ?res geo:spatialDistanceUnit "km" . # radius
 *   ?res geo:timePoint "1248885" .
 *   ?res geo:timeDistance "10" .
 *   ?res geo:timeDistanceUnit "s" .
 * }
 * </pre>
 * 
 * The query returns all points falling into the circle defined by radius 0.01
 * around the specified latitude and longitude for 1248885 +/- 10s.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface GeoSpatial {
   
   /**
    * A spatial unit.
    */
   public static enum SpatialUnit {
      FOOT,
      METER,
      KILOMETER,
      MILE
   }

   /**
    * A time unit.
    */
   public static enum TimeUnit {
      SECOND,
      MINUTE,
      HOUR,
      DAY
   }
   
   /**
    * Enum for implemented geo functions.
    */
   public static enum GeoFunction {
      IN_CIRCLE("inCircle");
      
      GeoFunction(final String name) {
         this.name = name;
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
      
      GeoFunction DEFAULT_GEO_FUNCTION = GeoFunction.IN_CIRCLE;
      
      /**
       * Option that may be set to specify a default for {@link GeoSpatial#SPATIAL_DISTANCE_UNIT},
       * to be used in geo service. Defaults to meters.
       */
      String GEO_SPATIAL_DISTANCE_UNIT = GeoSpatial.class.getName() + ".defaultSpatialDistanceUnit";
      
      SpatialUnit DEFAULT_GEO_SPATIAL_DISTANCE_UNIT = SpatialUnit.METER;

      /**
       * Option that may be set to specify a default for {@link GeoSpatial#TIME_DISTANCE_UNIT},
       * to be used in geo service. Defaults to minutes.
       */
      String GEO_TIME_DISTANCE_UNIT = GeoSpatial.class.getName() + ".defaulTimeDistanceUnit";
      
      TimeUnit DEFAULT_GEO_TIME_DISTANCE_UNIT = TimeUnit.SECOND;

   }
   
   
   /**
    * The namespace used for magic search predicates.
    */
   final String NAMESPACE = "http://www.bigdata.com/rdf/geospatial#";
   
   /**
    * The datatype to be used for GeoSpatial literals.
    */
   final URI DATATYPE = new URIImpl(NAMESPACE + "geoSpatialLiteral");
   
   final URI SEARCH = new URIImpl(NAMESPACE + "search");
   final URI SPATIAL_POINT = new URIImpl(NAMESPACE + "spatialPoint");
   final URI SPATIAL_DISTANCE = new URIImpl(NAMESPACE + "spatialDistance");
   final URI SPATIAL_DISTANCE_UNIT = new URIImpl(NAMESPACE + "spatialDistanceUnit");
   final URI TIME_POINT = new URIImpl(NAMESPACE + "timePoint");
   final URI TIME_DISTANCE = new URIImpl(NAMESPACE + "timeDistance");
   final URI TIME_DISTANCE_UNIT = new URIImpl(NAMESPACE + "timeDistanceUnit");
   


}