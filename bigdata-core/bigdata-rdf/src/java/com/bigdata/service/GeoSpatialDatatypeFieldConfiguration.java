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
 * Configuration of a single field/component in a given geospatial (multi-dimensional)
 * custom data type.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialDatatypeFieldConfiguration {
    
    final static private Logger log = Logger.getLogger(GeoSpatialDatatypeFieldConfiguration.class);


    /**
     * We support fields of values LONG and DOUBLE right now. Note that DOUBLE
     * values are internally converted to LONG based on the given precision.
     */
    public enum ValueType {
       LONG,
       DOUBLE
    }

    /**
     * The valueType of the field.
     */
    private final ValueType valueType;

    /**
     * The minimum value definig a shift in the bits.
     */
    private final long minValue;

    /**
     * The multiplier (aka precision)
     */
    private final long multiplier; // aka precision
    
    /**
     * Mapping to the service.
     */
    private final String serviceMapping;
        
    

    public GeoSpatialDatatypeFieldConfiguration(String uri, JSONObject fieldJson) throws IllegalArgumentException {
        
        
        try {
                // JSON struct: { "valueType": "double", "multiplier": "100000", "serviceMapping": "latitude" }
            valueType = ValueType.valueOf((String)fieldJson.get("valueType"));
            minValue = Long.valueOf((String)fieldJson.get("minVal"));
            multiplier = Long.valueOf((String)fieldJson.get("multiplier"));
            serviceMapping = (String)fieldJson.get("serviceMapping");
                
                
//                System.err.println("valueType=" + valueType +  " / multiplier=" + multiplier + " / serviceMapping=" + serviceMapping);
        } catch (NumberFormatException e) {
            
            log.warn("Expecting values that are of type long for minValue and multiplier.");
            throw new IllegalArgumentException(e);
            
        } catch (JSONException e) {
                
            log.warn("Field could not be parsed: " + e.getMessage());
            throw new IllegalArgumentException(e);
            
        } catch (Exception e) {
            
            log.warn("Exception while initializing field: " + e.getMessage());
            throw new IllegalArgumentException(e);
        }
                
    }
    
    

    public ValueType getValueType() {
        return valueType;
    }



    public long getMinValue() {
        return minValue;
    }



    public long getMultiplier() {
        return multiplier;
    }



    public String getServiceMapping() {
        return serviceMapping;
    }

}
