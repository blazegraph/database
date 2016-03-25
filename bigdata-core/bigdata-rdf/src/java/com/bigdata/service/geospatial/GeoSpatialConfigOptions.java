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
 * Created on March 25, 2016
 */
package com.bigdata.service.geospatial;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class hosting options for geospatial configuration and defaults.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialConfigOptions {

    /**
     * Options understood by the geospatial submodule.
     */
    public interface Options {
        
        /**
         * Enable GeoSpatial support. See https://jira.blazegraph.com/browse/BLZG-249.
         */
        String GEO_SPATIAL = AbstractTripleStore.class.getName() + ".geoSpatial";
      
        String DEFAULT_GEO_SPATIAL = "false";
        
        /**
         * Whether or not to include the built-in datatypes or not.
         */
        String GEO_SPATIAL_INCLUDE_BUILTIN_DATATYPES = AbstractTripleStore.class.getName() + ".geoSpatialIncludeBuiltinDatatypes";
      
        String DEFAULT_GEO_SPATIAL_INCLUDE_BUILTIN_DATATYPES = "true";

        /**
         * Return the geospatial default datatype (if any). The default datatype is the datatype that is queried
         * whenever no datatype is explicitly specified within the query.
         */
        String GEO_SPATIAL_DEFAULT_DATATYPE = AbstractTripleStore.class.getName() + ".geoSpatialDefaultDatatype";
        
        String DEFAULT_GEO_SPATIAL_DEFAULT_DATATYPE = null; // none

        
        /**
         * GeoSpatial builtin datatype definitions. Do *NOT* modify these definitions -> doing so will break
         * existing systems that make use of these built-in datatypes.
         */
        String GEO_SPATIAL_DATATYPE_CONFIG = AbstractTripleStore.class.getName() + ".geoSpatialDatatypeConfig";

        /**
         * ATTENTION: Do NEVER modify the definitions below -> they are important in order to maintain
         *            compatibility for geospatial datatypes that we expose as built-in datatypes.
         *            
         * See https://wiki.blazegraph.com/wiki/index.php/GeoSpatial#Custom_Geospatial_Datatypes for a
         * documentation of the JSON syntax. The JSON is parsed into instances by using the constructor
         * of {@link GeoSpatialConfig}, which takes a JSON string -- so you may also have a look at the
         * parsing logics there.
         */
        final String GEO_SPATIAL_LITERAL_V1_LAT_LON_CONFIG = 
                "{\"config\": "
                + "{ \"uri\": \"" + GeoSpatial.GEOSPATIAL_LITERAL_V1_LAT_LON + "\", "
                + "\"fields\": [ "
                + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" } "
                + "]}}";        
        
        final String GEO_SPATIAL_LITERAL_V1_LAT_LON_TIME_CONFIG = 
            "{\"config\": "
            + "{ \"uri\": \"" + GeoSpatial.GEOSPATIAL_LITERAL_V1_LAT_LON_TIME + "\", "
            + "\"fields\": [ "
            + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" }, "
            + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
            + "{ \"valueType\": \"LONG\", \"serviceMapping\" : \"TIME\"  } "
            + "]}}";
        // END ATTENTION

    }
}
