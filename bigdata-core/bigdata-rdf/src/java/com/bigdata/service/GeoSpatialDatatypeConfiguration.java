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
package com.bigdata.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription.Datatype;

/**
 * Configuration of a single geospatial datatype, including value type, multiplier,
 * min possible value, and mapping to the service.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialDatatypeConfiguration {
    
    final static private Logger log = Logger.getLogger(GeoSpatialDatatypeConfiguration.class);

    // URI of the datatype, e.g. <http://my.custom.geospatial.coordinate>
    private final String uri;

    
    private final Datatype datatype;

    public Long minValue; // the minimun value appearing in the data

    /**
     * Precision of the value, given as a multiplicator applied on top
     * of a given value. Typically precisions are 1000, meaning for instance
     * that we consider 3 after decimal positions for doubles. The precision
     * field can be used as well to shift long values, e.g. to align them
     * more closely in the index.
     */
    private final long precision;
        
    

    public GeoSpatialDatatypeConfiguration(String uri, JSONArray fieldsJson) {
        
        this.uri = uri;
        
        fields = new ArrayList<SchemaFieldDescription>();
        
        for (int i=0; i<fieldsJson.length(); i++) {
            

            try {
                // { "valueType": "double", "multiplier": "100000", "serviceMapping": "latitude" }
                JSONObject fieldJson = (JSONObject)fieldsJson.getJSONObject(i);
                
                String valueTypeStr = (String)fieldJson.get("valueType");
                String multiplierStr = (String)fieldJson.get("multiplier");
                String minValStr = (String)fieldJson.get("minVal");
                String serviceMappingStr = (String)fieldJson.get("serviceMapping");
                

//                SchemaFieldDescription sfd = new SchemaFieldDescription(datatype, precision, minValue)
                

//                System.err.println("valueType=" + valueType +  " / multiplier=" + multiplier + " / serviceMapping=" + serviceMapping);

            } catch (JSONException e) {
                
                log.warn("Field could not be parsed: " + e.getMessage());
                continue; // skip this element
                
            }
        }
            
        
                
        // TODO Auto-generated constructor stub
    }

}
