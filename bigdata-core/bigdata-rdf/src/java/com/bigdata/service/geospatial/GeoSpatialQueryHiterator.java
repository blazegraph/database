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

import java.util.Iterator;

/**
 * Visits geospatial query search result
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialQueryHiterator implements Iterator<IGeoSpatialQueryHit<?>> {

   /**
    * The index into the array of hits wrapped by this iterator.
    */
   private int rank = 0;

   /**
    * The array of hits wrapped by this iterator.
    */
   private final IGeoSpatialQueryHit<?>[] hits;

   /**
    * 
    * @param hits
    */
   public GeoSpatialQueryHiterator(final IGeoSpatialQueryHit<?>[] hits) {

      if (hits == null)
         throw new IllegalArgumentException();

      this.hits = hits;

   }

   public boolean hasNext() {

      return rank < hits.length;

   }

   public IGeoSpatialQueryHit<?> next() {

      return hits[rank++];

   }

   /**
    * @throws UnsupportedOperationException
    */
   public void remove() {

      throw new UnsupportedOperationException();

   }

   public String toString() {

      return "GeoSpatialSearchHiterator{nhits=" + hits.length + "} : " + hits;

   }

}
