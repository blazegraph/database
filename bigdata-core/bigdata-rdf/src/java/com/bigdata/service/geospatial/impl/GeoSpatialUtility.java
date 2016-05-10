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
 * Created on August 7, 2015
 */
package com.bigdata.service.geospatial.impl;

import com.bigdata.rdf.internal.gis.CoordinateDD;

/**
 * GeoSpatial utility functions.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialUtility {

   /**
    * A two dimensional point representing latitude and longitude. Wraps
    * the {@link CoordinateDD} with some use case specific functionality.
    */
   public static class PointLatLon {
      
      public static final String POINT_SEPARATOR = "#";
      
      final CoordinateDD point;
      
      public PointLatLon(CoordinateDD point) {
         this.point = point;
      }
      
      /**
       * Construction from string.
       */
      public PointLatLon(final String s) throws NumberFormatException {
         
         if (s==null || s.isEmpty()) {
            throw new NumberFormatException("Point is null or empty.");
         }
         
         String[] coords = s.split(POINT_SEPARATOR);
         if (coords.length!=2) {
            throw new NumberFormatException("Point must have 2 components, but has " + coords.length);
         }

         this.point = new CoordinateDD(
            Double.valueOf(coords[0]), Double.valueOf(coords[1]));
      }

      /**
       * Construction from data.
       */
      public PointLatLon(final Double lat, final Double lon) {
         
         this.point = new CoordinateDD(lat, lon);         
      }
      
      public Double getLat() {
         return point.northSouth;
      }
      

      public Double getLon() {
         return point.eastWest;
      }
      
      public CoordinateDD asCoordinateDD() {
         return point;
      }
      
      @Override
      public String toString() {
         final StringBuffer buf = new StringBuffer();
         buf.append(getLat());
         buf.append("#");
         buf.append(getLon());
         return buf.toString();
      }
      
      /**
       * Splits a given point into its component string. Corresponds to a
       * parsed (and typed) version of what toString returns.
       * 
       * @param point
       * @return
       */
      public static Object[] toComponentString(PointLatLon point) {
         
         final Object[] components = {
               point.getLat(),
               point.getLon(),
         };
         
         return components;
      }
   }
   
   /**
    * A three dimensional point representing latitude, longitude, and time.
    */
   public static class PointLatLonTime {
      
      public static final String POINT_SEPARATOR = "#";

      private final PointLatLon spatialPoint;
      private final Long timestamp;
      
      /**
       * Construction from string.
       */
      public PointLatLonTime(final String s) throws NumberFormatException {
         
         if (s==null || s.isEmpty()) {
            throw new NumberFormatException("Point is null or empty.");
         }
         
         String[] xyCoord = s.split(POINT_SEPARATOR);
         if (xyCoord.length!=3) {
            throw new NumberFormatException(
               "Point must have 2 components, but has " + xyCoord.length);
         }

         spatialPoint = 
            new PointLatLon(Double.valueOf(xyCoord[0]), Double.valueOf(xyCoord[1]));
         timestamp = Long.valueOf(xyCoord[2]);
         
      }
      
      /**
       * Construction from data.
       */
      public PointLatLonTime(
         final CoordinateDD spatialPoint, final Long timestamp) {
         
         this(new PointLatLon(spatialPoint), timestamp);
      }
      
      /**
       * Construction from data.
       */
      public PointLatLonTime(
         final PointLatLon spatialPoint, final Long timestamp) {
         
         this.spatialPoint = spatialPoint;
         this.timestamp = timestamp;
      }

      
      /**
       * Construction from data.
       */
      public PointLatLonTime(
         final Double xCoord, final Double yCoord, final Long timestamp) {
         
         this(new PointLatLon(xCoord, yCoord),timestamp);
         
      }

      public PointLatLon getSpatialPoint() {
         return spatialPoint;
      }
      
      
      public Double getLat() {
         return spatialPoint.getLat();
      }
      

      public Double getLon() {
         return spatialPoint.getLon();
      }
      
      public Long getTimestamp() {
         return timestamp;
      }
      
      @Override
      public String toString() {
         final StringBuffer buf = new StringBuffer();
         buf.append(getLat());
         buf.append("#");
         buf.append(getLon());
         buf.append("#");
         buf.append(getTimestamp());
         return buf.toString();
      }
      
      /**
       * Splits a given point into its component string. Corresponds to a
       * parsed (and typed) version of what toString returns.
       * 
       * @param point
       * @return
       */
      public static Object[] toComponentString(PointLatLonTime point) {
         
         final Object[] components = {
               point.getSpatialPoint().getLat(),
               point.getSpatialPoint().getLon(),
               point.getTimestamp()
         };
         
         return components;
      }
   }

   
   /**
    * A bounding box over a {@link PointLatLonTime}.
    */
   public static class BoundingBoxLatLonTime {
      
      private final PointLatLonTime centerPoint;
      
      private final Double distanceLat;
      
      private final Double distanceLon;
      
      private final Long distanceTime;
      
      /** 
       * Constructor setting up a bounding box for the specified data.
       */
      public BoundingBoxLatLonTime(
         final PointLatLonTime centerPoint, final Double distanceLat,
         final Double distanceLon, final Long distanceTime) {
         
         this.centerPoint = centerPoint;
         this.distanceLat = distanceLat;
         this.distanceLon = distanceLon;
         this.distanceTime = distanceTime;
      }
      
      public PointLatLonTime getLowerBorder() {
         
         return new PointLatLonTime(
              centerPoint.getSpatialPoint().getLat() - distanceLat, 
              centerPoint.getSpatialPoint().getLon() - distanceLon,
              centerPoint.getTimestamp() - distanceTime);
         
      }
      
      public PointLatLonTime getUpperBorder() {
         
         return new PointLatLonTime(
            centerPoint.getSpatialPoint().getLat() + distanceLat, 
            centerPoint.getSpatialPoint().getLon() + distanceLon,
            centerPoint.getTimestamp() + distanceTime);
      }
         
      @Override
      public String toString() {
         
         
         final StringBuffer buf = new StringBuffer();
         buf.append("Center point: ");
         buf.append(centerPoint);
         buf.append("\n");
         
         buf.append("Distance (lat/lon/time): ");
         buf.append(distanceLat);
         buf.append("/");
         buf.append(distanceLon);
         buf.append("/");
         buf.append(distanceTime);
         buf.append("\n");
         
         buf.append("-> Low border: ");
         buf.append(getLowerBorder());
         buf.append("\n");
         
         buf.append("-> High border: ");
         buf.append(getUpperBorder());
         buf.append("\n");
         
         return buf.toString();
      }
   }
   
}
