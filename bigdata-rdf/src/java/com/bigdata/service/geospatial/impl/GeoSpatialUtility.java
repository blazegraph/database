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
 * Created on August 7, 2015
 */
package com.bigdata.service.geospatial.impl;

/**
 * GeoSpatial utility functions.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialUtility {


   /**
    * A two dimensional point representing latitude and longitude.
    */
   public static class PointLatLon {
      
      public static final String POINT_SEPARATOR = "#";

      private final Double xCoord;
      private final Double yCoord;
      
      /**
       * Construction from string.
       */
      public PointLatLon(final String s) throws NumberFormatException {
         
         if (s==null || s.isEmpty()) {
            throw new NumberFormatException("Point is null or empty.");
         }
         
         String[] xyCoord = s.split(POINT_SEPARATOR);
         if (xyCoord.length!=2) {
            throw new NumberFormatException("Point must have 2 components, but has " + xyCoord.length);
         }
         
         xCoord = Double.valueOf(xyCoord[0]);
         yCoord = Double.valueOf(xyCoord[1]);
      }

      /**
       * Construction from data.
       */
      public PointLatLon(final Double xCoord, final Double yCoord) {
         this.xCoord = xCoord;
         this.yCoord = yCoord;
      }
      
      public Double getXCoord() {
         return xCoord;
      }
      

      public Double getYCoord() {
         return yCoord;
      }
      
      @Override
      public String toString() {
         final StringBuffer buf = new StringBuffer();
         buf.append(xCoord);
         buf.append("#");
         buf.append(yCoord);
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
               point.getXCoord(),
               point.getYCoord(),
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
      
      public Long getTimestamp() {
         return timestamp;
      }

      @Override
      public String toString() {
         final StringBuffer buf = new StringBuffer();
         buf.append(spatialPoint.xCoord);
         buf.append("#");
         buf.append(spatialPoint.yCoord);
         buf.append("#");
         buf.append(timestamp);
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
               point.getSpatialPoint().getXCoord(),
               point.getSpatialPoint().getYCoord(),
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
              centerPoint.getSpatialPoint().getXCoord() - distanceLat, 
              centerPoint.getSpatialPoint().getYCoord() - distanceLon,
              centerPoint.getTimestamp() - distanceTime);
         
      }
      
      public PointLatLonTime getUpperBorder() {
         
         return new PointLatLonTime(
            centerPoint.getSpatialPoint().getXCoord() + distanceLat, 
            centerPoint.getSpatialPoint().getYCoord() + distanceLon,
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
