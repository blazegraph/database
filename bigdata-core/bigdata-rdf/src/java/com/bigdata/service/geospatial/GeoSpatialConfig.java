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
 * Created on Feb 10, 2016
 */
package com.bigdata.service.geospatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.impl.extensions.InvalidGeoSpatialDatatypeConfigurationError;

/**
 * Class providing access to the GeoSpatial index configuration,
 * including datatype definition and default datatype for querying.
 * Initialized and used only if geospatial subsytem is used.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static String JSON_STR_CONFIG = "config";
    private final static String JSON_STR_URI = "uri";
    private final static String JSON_STR_LITERALSERIALIZER = "literalSerializer";
    private final static String JSON_STR_FIELDS = "fields";

    final static private transient Logger log = Logger.getLogger(GeoSpatialConfig.class);

    /**
     * List containing the configurations for the geospatial datatypes.
     */
    private List<GeoSpatialDatatypeConfiguration> datatypeConfigs;

    
    // the default datatype for querying
    private URI defaultDatatype;

    
    public GeoSpatialConfig(final List<String> geoSpatialDatatypeConfigs, final String defaultDatatype) {

        initDatatypes(geoSpatialDatatypeConfigs);
        
        if (defaultDatatype!=null && !defaultDatatype.isEmpty()) {

            try {
                this.defaultDatatype = new URIImpl(defaultDatatype);
            } catch (Exception e) {
                throw new InvalidGeoSpatialDatatypeConfigurationError(
                    "Invalid default datatype (" + defaultDatatype + ") does not represent a URI.");
            }
            
            boolean isRegistered = false;
            for (final GeoSpatialDatatypeConfiguration config : datatypeConfigs) {
                isRegistered |= config.getUri().equals(this.defaultDatatype);
            }

            if (!isRegistered) {
                throw new InvalidGeoSpatialDatatypeConfigurationError(
                        "Invalid default datatype (" + defaultDatatype + ") is not a registered geospatial datatype.");
            }

        } // else: ignore it (no default)
        
    }
    
    private void initDatatypes(List<String> geoSpatialDatatypeConfigs) {
       
        datatypeConfigs = new ArrayList<GeoSpatialDatatypeConfiguration>();
       
        if (geoSpatialDatatypeConfigs==null)
            return; // nothing to be done

        /**
         * We expect a JSON config string of the following format (example):
         * 
         * {"config": { 
         *   "uri": "http://my.custom.datatype2.uri", 
         *   "literalSerializer": "com.bigdata.service.GeoSpatialLiteralSerializer",
         *   "fields": [ 
         *     { "valueType": "DOUBLE", "multiplier": "100000", "serviceMapping": "LATITUDE" }, 
         *     { "valueType": "DOUBLE", "multiplier": "100000", "serviceMapping": "LONGITUDE" }, 
         *     { "valueType": "LONG, "multiplier": "1", "minValue" : "0" , "serviceMapping": "TIME"  }, 
         *     { "valueType": "LONG", "multiplier": "1", "minValue" : "0" , "serviceMapping" : "COORD_SYSTEM"  } 
         *   ] 
         * }}
         */
        for (final String configStr : geoSpatialDatatypeConfigs) {
           
            if (configStr==null || configStr.isEmpty())
                continue; // skip

            try {

                // read values from JSON
                final JSONObject json = new JSONObject(configStr);
                final JSONObject topLevelNode = (JSONObject)json.get(JSON_STR_CONFIG);
                final String uri = (String)topLevelNode.get(JSON_STR_URI);
                final String literalSerializer = topLevelNode.has(JSON_STR_LITERALSERIALIZER) ?
                        (String)topLevelNode.get(JSON_STR_LITERALSERIALIZER) : null;
                final JSONArray fields = (JSONArray)topLevelNode.get(JSON_STR_FIELDS);

                // delegate to GeoSpatialDatatypeConfiguration for construction
                datatypeConfigs.add(new GeoSpatialDatatypeConfiguration(uri, literalSerializer, fields));
                
            } catch (JSONException e) {
                
                log.warn("Illegal JSON configuration: " + e.getMessage());
                throw new IllegalArgumentException(e); // forward exception
            }
           
            // validate that there are no duplicate URIs used for the datatypeConfigs
            final Set<URI> uris = new HashSet<URI>();
            for (int i=0; i<datatypeConfigs.size(); i++) {
                
                final URI curUri = datatypeConfigs.get(i).getUri();
                
                if (uris.contains(curUri)) {
                    throw new IllegalArgumentException("Duplicate URI used for geospatial datatype config: " + curUri);
                }
                
                uris.add(curUri);
            }

        }
    }
   
    public GeoSpatialDatatypeConfiguration getConfigurationForDatatype(URI datatypeUri) {
        for (int i=0; i<datatypeConfigs.size(); i++) {
            final GeoSpatialDatatypeConfiguration cur = datatypeConfigs.get(i);
            if (cur.getUri().equals(datatypeUri)) {
                return cur;
            }
        }
        
        return null; // not found/registered
    }
    
    public List<GeoSpatialDatatypeConfiguration> getDatatypeConfigs() {
        return datatypeConfigs;
    }

    public URI getDefaultDatatype() {
        return defaultDatatype;
    }

}
