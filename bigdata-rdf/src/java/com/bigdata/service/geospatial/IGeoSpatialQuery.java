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

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatial.SpatialUnit;
import com.bigdata.service.geospatial.GeoSpatial.TimeUnit;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;

/**
 * Interface for geospatial queries and their execution.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IGeoSpatialQuery {


   /**
    * Execute a GeoSpatial query.
    * 
    * @param query the query to execute
    * @param indexManager the index manager
    * @return a result iterator
    */
   public GeoSpatialQueryHiterator search(
      final GeoSpatialSearchQuery query, final AbstractTripleStore tripleStore);

   /**
    * Representation of a GeoSpatial query. See {@link GeoSpatial} service
    * for details.
    */
   public static class GeoSpatialSearchQuery implements Serializable {

      private static final long serialVersionUID = -2509557655519603130L;

      final GeoFunction searchFunction;
      final TermNode predicate;
      final PointLatLon spatialCircleCenter;
      final Double spatialCircleRadius;
      final PointLatLon spatialRectangleUpperLeft;
      final PointLatLon spatialRectangleLowerRight;
      final SpatialUnit spatialUnit;
      final Long timeStart;
      final Long timeEnd;
      final TimeUnit timeUnit;
      final IBindingSet incomingBindings;


      /**
       * Constructor
       */
      public GeoSpatialSearchQuery(final GeoFunction searchFunction, 
            final TermNode predicate, final PointLatLon spatialCircleCenter, 
            final Double spatialCircleRadius,
            PointLatLon spatialRectangleUpperLeft,
            PointLatLon spatialRectangleLowerRight,
            final SpatialUnit spatialUnit, final Long timeStart,
            final Long timeEnd, final TimeUnit timeUnit,
            final IBindingSet incomingBindings) {

         this.searchFunction = searchFunction;
         this.predicate = predicate;
         this.spatialCircleCenter = spatialCircleCenter;
         this.spatialCircleRadius = spatialCircleRadius;
         this.spatialRectangleUpperLeft = spatialRectangleUpperLeft;
         this.spatialRectangleLowerRight = spatialRectangleLowerRight;
         this.spatialUnit = spatialUnit;
         this.timeStart = timeStart;
         this.timeEnd = timeEnd;
         this.timeUnit = timeUnit;
         this.incomingBindings = incomingBindings;

      }
      
      public GeoFunction getSearchFunction() {
         return searchFunction;
      }

      public TermNode getPredicate() {
         return predicate;
      }

      public PointLatLon getSpatialCircleCenter() {
         return spatialCircleCenter;
      }

      public Double getSpatialCircleRadius() {
         return spatialCircleRadius;
      }

      public PointLatLon getSpatialRectangleUpperLeft() {
         return spatialRectangleUpperLeft;
      }

      public PointLatLon getSpatialRectangleLowerRight() {
         return spatialRectangleLowerRight;
      }

      public SpatialUnit getSpatialUnit() {
         return spatialUnit;
      }

      public Long getTimeStart() {
         return timeStart;
      }

      public Long getTimeEnd() {
         return timeEnd;
      }

      public TimeUnit getTimeUnit() {
         return timeUnit;
      }
      
      public IBindingSet getIncomingBindings() {
         return incomingBindings;
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
            ((predicate == null) ? 0 : predicate.hashCode());
         
         result = prime * result + 
            ((spatialCircleCenter == null) ? 0 : spatialCircleCenter.hashCode());
         
         result = prime * result + 
            ((spatialCircleRadius == null) ? 0 : spatialCircleRadius.hashCode());

         result = prime * result + 
            ((spatialRectangleUpperLeft == null) ? 0 : spatialRectangleUpperLeft.hashCode());

         result = prime * result + 
            ((spatialRectangleLowerRight == null) ? 0 : spatialRectangleLowerRight.hashCode());

         result = prime * result + 
            ((spatialUnit == null) ? 0 : spatialUnit.hashCode());

         result = prime * result + 
               ((timeStart == null) ? 0 : timeStart.hashCode());

         result = prime * result + 
               ((timeEnd == null) ? 0 : timeEnd.hashCode());

         result = prime * result + 
               ((timeUnit == null) ? 0 : timeUnit.hashCode());

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
         
         if ((predicate == null && other.predicate != null)
               || (predicate != null && other.predicate == null)
               || !predicate.equals(other.predicate))
            return false;

         if ((spatialCircleCenter == null && other.spatialCircleCenter != null)
               || (spatialCircleCenter != null && other.spatialCircleCenter == null)
               || !spatialCircleCenter.equals(other.spatialCircleCenter))
            return false;

         if ((spatialCircleRadius == null && other.spatialCircleRadius != null)
               || (spatialCircleRadius != null && other.spatialCircleRadius == null)
               || !spatialCircleRadius.equals(other.spatialCircleRadius))
            return false;

         if ((spatialRectangleUpperLeft == null && other.spatialRectangleUpperLeft != null)
               || (spatialRectangleUpperLeft != null && other.spatialRectangleUpperLeft == null)
               || !spatialRectangleUpperLeft.equals(other.spatialRectangleUpperLeft))
            return false;

         if ((spatialRectangleLowerRight == null && other.spatialRectangleLowerRight != null)
               || (spatialRectangleLowerRight != null && other.spatialRectangleLowerRight == null)
               || !spatialRectangleLowerRight.equals(other.spatialRectangleLowerRight))
            return false;

         if ((spatialUnit == null && other.spatialUnit != null)
               || (spatialUnit != null && other.spatialUnit == null)
               || !spatialUnit.equals(other.spatialUnit))
            return false;

         if ((timeStart == null && other.timeStart != null)
               || (timeStart != null && other.timeStart == null)
               || !timeStart.equals(other.timeStart))
            return false;

         if ((timeEnd == null && other.timeEnd != null)
               || (timeEnd != null && other.timeEnd == null)
               || !timeEnd.equals(other.timeEnd))
            return false;

         if ((timeUnit == null && other.timeUnit != null)
               || (timeUnit != null && other.timeUnit == null)
               || !timeUnit.equals(other.timeUnit))
            return false;

         return true;
      }

   }


}