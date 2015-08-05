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

import java.io.Serializable;

import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatial.Point2D;
import com.bigdata.service.geospatial.GeoSpatial.SpatialUnit;
import com.bigdata.service.geospatial.GeoSpatial.TimeUnit;

/**
 * Interface for geospatial queries and their execution.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IGeoSpatialQuery {


   public GeoSpatialQueryHiterator search(final GeoSpatialSearchQuery query);
   
   // public int count(final ExternalSolrSearchQuery query);

   public static class GeoSpatialSearchQuery implements Serializable {

      private static final long serialVersionUID = -2509557655519603130L;

      final GeoFunction searchFunction;
      final Point2D spatialPoint;
      final Double spatialDistance;
      final SpatialUnit spatialDistanceUnit;
      final Double timePoint;
      final Double timeDistance;
      final TimeUnit timeDistanceUnit;

      /**
       * Constructor
       */
      public GeoSpatialSearchQuery(final GeoFunction searchFunction, 
            final Point2D spatialPoint, final Double spatialDistance,
            final SpatialUnit spatialDistanceUnit, final Double timePoint,
            final Double timeDistance, final TimeUnit timeDistanceUnit) {

         this.searchFunction = searchFunction;
         this.spatialPoint = spatialPoint;
         this.spatialDistance = spatialDistance;
         this.spatialDistanceUnit = spatialDistanceUnit;
         this.timePoint = timePoint;
         this.timeDistance = timeDistance;
         this.timeDistanceUnit = timeDistanceUnit;

      }
      
      public GeoFunction getSearchFunction() {
         return searchFunction;
      }


      public Point2D getSpatialPoint() {
         return spatialPoint;
      }


      public Double getSpatialDistance() {
         return spatialDistance;
      }


      public SpatialUnit getSpatialDistanceUnit() {
         return spatialDistanceUnit;
      }


      public Double getTimePoint() {
         return timePoint;
      }


      public Double getTimeDistance() {
         return timeDistance;
      }


      public TimeUnit getTimeDistanceUnit() {
         return timeDistanceUnit;
      }
      
      /*
       * (non-Javadoc)
       * 
       * @see java.lang.Object#hashCode()
       */
      @Override
      public int hashCode() {
         final int prime = 31;

         int result = 1;
         
         result = prime * result + 
            ((searchFunction == null) ? 0 : searchFunction.hashCode());
         
         result = prime * result +
            ((spatialPoint == null) ? 0 : spatialPoint.hashCode());
         
         result = prime * result + 
            ((spatialDistance == null) ? 0 : spatialDistance.hashCode());
         
         result = prime * result + 
            ((spatialDistanceUnit == null) ? 0 : spatialDistanceUnit.hashCode());

         result = prime * result + 
            ((timePoint == null) ? 0 : timePoint.hashCode());

         result = prime * result + 
            ((timeDistance == null) ? 0 : timeDistance.hashCode());

         result = prime * result + 
            ((timeDistanceUnit == null) ? 0 : timeDistanceUnit.hashCode());

         return result;
      }

      /*
       * (non-Javadoc)
       * 
       * @see java.lang.Object#equals(java.lang.Object)
       */
      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         GeoSpatialSearchQuery other = (GeoSpatialSearchQuery) obj;

         if ((searchFunction == null && other.searchFunction != null)
               || (searchFunction != null && other.searchFunction == null)
               || !searchFunction.equals(other.searchFunction))
            return false;

         if ((spatialPoint == null && other.spatialPoint != null)
               || (spatialPoint != null && other.spatialPoint == null)
               || !spatialPoint.equals(other.spatialPoint))
            return false;

         if ((spatialDistance == null && other.spatialDistance != null)
               || (spatialDistance != null && other.spatialDistance == null)
               || !spatialDistance.equals(other.spatialDistance))
            return false;

         if ((spatialDistanceUnit == null && other.spatialDistanceUnit != null)
               || (spatialDistanceUnit != null && other.spatialDistanceUnit == null)
               || !spatialDistanceUnit.equals(other.spatialDistanceUnit))
            return false;

         if ((timePoint == null && other.timePoint != null)
               || (timePoint != null && other.timePoint == null)
               || !timePoint.equals(other.timePoint))
            return false;

         if ((timeDistanceUnit == null && other.timeDistanceUnit != null)
               || (timeDistanceUnit != null && other.timeDistanceUnit == null)
               || !timeDistanceUnit.equals(other.timeDistanceUnit))
            return false;

         return true;
      }

   }


}