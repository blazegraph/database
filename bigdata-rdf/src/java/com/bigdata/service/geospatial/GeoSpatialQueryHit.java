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

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataURI;


/**
 * Single result for a geospatial query.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialQueryHit<V extends Comparable<V>> implements 
   IGeoSpatialQueryHit<V>,Comparable<GeoSpatialQueryHit<V>> {

   protected final BigdataURI res;
   protected final IBindingSet incomingBindings;

   
   
   public GeoSpatialQueryHit(
      final BigdataURI res, final IBindingSet incomingBindings) {
      
      this.res = res;
      this.incomingBindings = incomingBindings;
   }

   public String toString() {

      return "FulltextSearchHit{res=" + res + "}";

   }

   /**
    */
   public int compareTo(final GeoSpatialQueryHit<V> o) {

      // TODO: implement / check
      return res.getIV().compareTo(o.getRes().getIV());

   }
   
   @Override
   public BigdataURI getRes() {
      return res;
   }

   @Override
   public IBindingSet getIncomingBindings() {
      return incomingBindings;
   }

}
