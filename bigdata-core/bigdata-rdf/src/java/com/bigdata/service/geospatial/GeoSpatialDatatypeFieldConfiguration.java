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

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.rdf.internal.impl.extensions.InvalidGeoSpatialDatatypeConfigurationError;

/**
 * Configuration of a single field/component in a given geospatial (multi-dimensional)
 * custom data type.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialDatatypeFieldConfiguration implements Serializable {
    
    private static final long serialVersionUID = 1L;

    final static private transient Logger log = 
        Logger.getLogger(GeoSpatialDatatypeFieldConfiguration.class);
    
    private final static String JSON_STR_VALUETYPE = "valueType";
    private final static String JSON_STR_MINVALUE = "minValue";
    private final static String JSON_STR_MULTIPLIER = "multiplier";
    private final static String JSON_STR_SERVICEMAPPING = "serviceMapping";

    /**
     * We support fields of values LONG and DOUBLE right now. Note that DOUBLE
     * values are internally converted to LONG based on the given precision.
     */
    public enum ValueType {
       LONG,
       DOUBLE
    }
    
    /**
     * Service mapping.
     */
    public enum ServiceMapping {
        LATITUDE,
        LONGITUDE,
        TIME,
        COORD_SYSTEM,
        
        CUSTOM
    }

    /**
     * The valueType of the field.
     */
    private final ValueType valueType;

    /**
     * The minimum value defining a shift in the bits. May be null (meaning there is no minValue defined).
     */
    private Long minValue;

    /**
     * The multiplier (aka precision). Defaults to 1 (identity element for multiplication).
     */
    private long multiplier;
    
    /**
     * Mapping to the service. If serviceMapping==CUSTOM, this is a string
     * defined in variable customServiceMapping, otherwise the latter is null.
     */
    private ServiceMapping serviceMapping = ServiceMapping.CUSTOM; // default
    
    /**
     * Custom service mapping string.
     */
    private String customServiceMapping;
    

    public GeoSpatialDatatypeFieldConfiguration(JSONObject fieldJson) throws InvalidGeoSpatialDatatypeConfigurationError {
        

        /**
         * Parse JSON structure such as:
         * { "valueType": "LONG", "multiplier": "1", "minVal" : "0" , "serviceMapping" : "COORD_SYSTEM"  },
         * where minVal and multiplier are optional, the other two components are mandatory.
         */
        try {

            // component 1: valueType (required)
            valueType = ValueType.valueOf((String)fieldJson.get(JSON_STR_VALUETYPE));
            
            // component 2: minValue (optional)
            minValue=null; // default
            if (fieldJson.has(JSON_STR_MINVALUE)) {
                final String minValueStr = (String)fieldJson.get(JSON_STR_MINVALUE);
                if (minValueStr!=null && !minValueStr.isEmpty()) {
                    minValue = Long.valueOf(minValueStr);
                } // else: stay with default
            }
            
            // component 3: multiplier (optional)            
            multiplier=1; // default
            if (fieldJson.has(JSON_STR_MULTIPLIER)) {
                final String multiplierStr = (String)fieldJson.get(JSON_STR_MULTIPLIER);
                if (multiplierStr!=null && !multiplierStr.isEmpty()) {
                    multiplier = Long.valueOf(multiplierStr);
                }
            }
            
            // component 4: serviceMapping (required)
            final String serviceMappingStr = (String)fieldJson.get(JSON_STR_SERVICEMAPPING);
            try {
                
                serviceMapping = ServiceMapping.valueOf((String)fieldJson.get(JSON_STR_SERVICEMAPPING));
                customServiceMapping = null;
                
            } catch (Exception e) {
             
                customServiceMapping = serviceMappingStr;
                        
            }

        } catch (NumberFormatException e) {
            
            log.warn("Expecting values that are of type long for minValue and multiplier.");
            throw new InvalidGeoSpatialDatatypeConfigurationError(e.getMessage()); // forward exception
            
        } catch (JSONException e) {
                
            log.warn("Field could not be parsed: " + e.getMessage());
            throw new InvalidGeoSpatialDatatypeConfigurationError(e.getMessage()); // forward exception
            
        } catch (Exception e) {
            
            log.warn("Exception while initializing field: " + e.getMessage());
            throw new InvalidGeoSpatialDatatypeConfigurationError(e.getMessage()); // forward exception
        }
                
    }
    
    /**
     * Alternative constructor (to ease writing test cases). If used without test cases,
     * please use with care -- this constructor does not implement input validation.
     * 
     * @param uri
     * @param fields
     */
    public GeoSpatialDatatypeFieldConfiguration(
        final ValueType valueType, final Long minValue, 
        final long multiplier, final ServiceMapping serviceMapping,
        final String customServiceMapping) {
        
        this.valueType = valueType;
        this.minValue = minValue;
        this.multiplier = multiplier;
        this.serviceMapping = serviceMapping;
        this.customServiceMapping = customServiceMapping;
    }

    public ValueType getValueType() {
        return valueType;
    }



    public Long getMinValue() {
        return minValue;
    }



    public long getMultiplier() {
        return multiplier;
    }



    public ServiceMapping getServiceMapping() {
        return serviceMapping;
    }

    
    public String getCustomServiceMapping() {
        return customServiceMapping;
    }

}
