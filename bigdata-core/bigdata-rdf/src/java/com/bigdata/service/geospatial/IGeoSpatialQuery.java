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
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
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


      /**
       * Constructor
       */
      public GeoSpatialSearchQuery(final GeoFunction searchFunction, 
            final IConstant<?> subject, final TermNode predicate, 
            final TermNode context, final PointLatLon spatialCircleCenter, 
            final Double spatialCircleRadius,
            PointLatLon spatialRectangleUpperLeft,
            PointLatLon spatialRectangleLowerRight,
            final UNITS spatialUnit, final Long timeStart,
            final Long timeEnd, final IVariable<?> locationVar,
            final IVariable<?> timeVar, final IVariable<?> locationAndTimeVar, 
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

      }
      
      public GeoFunction getSearchFunction() {
         return searchFunction;
      }
      
      public IConstant<?> getSubject() {
         return subject;
      }

      public TermNode getPredicate() {
         return predicate;
      }

      public TermNode getContext() {
          return context;
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

      public UNITS getSpatialUnit() {
         return spatialUnit;
      }

      public Long getTimeStart() {
         return timeStart;
      }

      public Long getTimeEnd() {
         return timeEnd;
      }
      
      public IVariable<?> getLocationVar() {
         return locationVar;
      }

      public IVariable<?> getTimeVar() {
         return timeVar;
      }

      public IVariable<?> getLocationAndTimeVar() {
         return locationAndTimeVar;
      }

      
      public IBindingSet getIncomingBindings() {
         return incomingBindings;
      }

   }

}