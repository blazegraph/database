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
 * Metadata of a geospatial query result. See {@link GeoSpatial}, including
 * the extracted subjects.
 *            
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IGeoSpatialQueryHit<V extends Comparable<V>> {

   /**
    * The result of the geospatial search.
    */
   public BigdataURI getRes();
    
   /**
    * Get the set of incoming bindings for the search hit.
    */
   public IBindingSet getIncomingBindings();
    
}
