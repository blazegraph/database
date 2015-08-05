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
package com.bigdata.service.geospatial.impl;

import org.apache.log4j.Logger;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.service.geospatial.GeoSpatialQueryHiterator;
import com.bigdata.service.geospatial.IGeoSpatialQuery;

/**
 * Implementation of geospatial queries against the (internal) geospatial
 * index (which is incorporated in the standard BTree index)
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialQueryImpl implements IGeoSpatialQuery {

   final private static transient Logger log = Logger
         .getLogger(GeoSpatialQueryImpl.class);



   @Override
   public GeoSpatialQueryHiterator search(GeoSpatialSearchQuery query) {
      
      System.out.println("Here we go...");
      // TODO: implement
      GeoSpatialQueryHiterator hiterator = new GeoSpatialQueryHiterator(new BigdataURI[0]); 
      return hiterator;

   }
  
}
