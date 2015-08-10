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
 * 
 * Example: find all points in the rectangle spanned from (0.00001, 0.00002)
 * to (0.00003, 0.00003) with times between 1-3s.
 * 
 * <pre>
   PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
   SELECT ?res WHERE {
     ?res geo:search "inRectangle" .
     ?res geo:predicate <http://o> .
     ?res geo:spatialRectangleUpperLeft "0.00001#0.00002" .
     ?res geo:spatialRectangleLowerRight "0.00003#0.00003" .
     ?res geo:timeStart "1" .
     ?res geo:timeEnd "3" .
     ?res geo:timeUnit "s" .
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
     ?res geo:timeUnit "s" .
   }
 * </pre>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface GeoSpatial {
   
   /**
    * A spatial unit.
    */
   public static enum SpatialUnit {
      
      FOOT("ft"),
      METER("m"),
      KILOMETER("km"),
      MILE("mi");
      
      SpatialUnit(String name) {
         this.name = name;
      }
      
      public static SpatialUnit forName(String name) {
         for (SpatialUnit e : SpatialUnit.values()) {
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

   /**
    * A time unit.
    */
   public static enum TimeUnit {
      SECOND("s"),
      MINUTE("m"),
      HOUR("h"),
      DAY("d");
      
      TimeUnit(String name) {
         this.name = name;
      }
      
      public static TimeUnit forName(String name) {
         for (TimeUnit e : TimeUnit.values()) {
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
   
   /**
    * Enum for implemented geo functions.
    */
   public static enum GeoFunction {
      IN_CIRCLE("inCircle"),
      IN_RECTANGLE("inRectangle");
      
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

   // TODO: documentation of vocabulary once finalized
   final URI SEARCH = new URIImpl(NAMESPACE + "search");
   final URI PREDICATE = new URIImpl(NAMESPACE + "predicate");
   final URI SPATIAL_CIRCLE_CENTER = new URIImpl(NAMESPACE + "spatialCircleCenter");
   final URI SPATIAL_CIRCLE_RADIUS = new URIImpl(NAMESPACE + "spatialCircleRadius");
   final URI SPATIAL_RECTANGLE_UPPER_LEFT = new URIImpl(NAMESPACE + "spatialRectangleUpperLeft");
   final URI SPATIAL_RECTANGLE_LOWER_RIGHT = new URIImpl(NAMESPACE + "spatialRectangleLowerRight");
   final URI SPATIAL_UNIT = new URIImpl(NAMESPACE + "spatialUnit");
   final URI TIME_START = new URIImpl(NAMESPACE + "timeStart");
   final URI TIME_END = new URIImpl(NAMESPACE + "timeEnd");
   final URI TIME_UNIT = new URIImpl(NAMESPACE + "timeUnit");
   
}