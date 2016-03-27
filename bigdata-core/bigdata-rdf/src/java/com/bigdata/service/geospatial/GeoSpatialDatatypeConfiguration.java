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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.impl.extensions.InvalidGeoSpatialDatatypeConfigurationError;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration.ServiceMapping;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration.ValueType;

/**
 * Configuration of a single geospatial datatype, including value type, multiplier,
 * min possible value, and mapping to the service.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialDatatypeConfiguration implements Serializable {
    
    private static final long serialVersionUID = 1L;

    final static private transient Logger log = 
        Logger.getLogger(GeoSpatialDatatypeConfiguration.class);

    // URI of the datatype, e.g. <http://my.custom.geospatial.coordinate>
    private final URI uri;
    
    private final IGeoSpatialLiteralSerializer literalSerializer;

    // ordered list of fields defining the datatype
    private List<GeoSpatialDatatypeFieldConfiguration> fields;

    // derived members
    private boolean hasLat = false;
    private boolean hasLon = false;
    private boolean hasTime = false;
    private boolean hasCoordSystem = false;
    private Map<String,Integer> customFieldsIdxs = new HashMap<String,Integer>();


    /**
     * Constructor, setting up a {@link GeoSpatialDatatypeConfiguration} given a uri and a
     * JSON array defining the fields as input. Throws an {@link InvalidGeoSpatialDatatypeConfigurationError}
     * if the uri is null or empty or in case the JSON array does not describe a set of
     * valid fields.
     */
    public GeoSpatialDatatypeConfiguration(
        final String uriStr, final String literalSerializerClass, final JSONArray fieldsJson) {
        
        if (uriStr==null || uriStr.isEmpty())
            throw new InvalidGeoSpatialDatatypeConfigurationError("URI parameter must not be null or empty");
        
        try {
            this.uri = new URIImpl(uriStr);
        } catch (Exception e) {
            throw new InvalidGeoSpatialDatatypeConfigurationError("Invalid URI in geospatial datatype config: " + uriStr);
        }
        
        if (literalSerializerClass==null || literalSerializerClass.isEmpty()) {
            literalSerializer = new GeoSpatialDefaultLiteralSerializer();
        } else {
            try {
                Class<?> literalSerializerClazz = Class.forName(literalSerializerClass);
                try {
                    
                    final Object instance = literalSerializerClazz.newInstance();
                    if (!(instance instanceof IGeoSpatialLiteralSerializer)) {
                        throw new InvalidGeoSpatialDatatypeConfigurationError("Literal serializer class " 
                                + literalSerializerClass + 
                                " does not implement GeoSpatialLiteralSerializer interface.");
                    }
                    
                    literalSerializer = (IGeoSpatialLiteralSerializer)instance;
                    
                } catch (Exception e) {
                    throw new InvalidGeoSpatialDatatypeConfigurationError(
                            "Literal serializer class " + literalSerializerClass + 
                            " could not be instantiated: " + e.getMessage());
                }
                
            } catch (ClassNotFoundException e) {
                throw new InvalidGeoSpatialDatatypeConfigurationError(
                    "Literal serializer class not found: " + literalSerializerClass);

            }
            
        }
        
        fields = new ArrayList<GeoSpatialDatatypeFieldConfiguration>();
        
        /**
         * We expect a JSON array of the following format (example):
         * 
         * [ 
         *   { "valueType": "DOUBLE", "multiplier": "100000", "serviceMapping": "LATITUDE" }, 
         *   { "valueType": "DOUBLE", "multiplier": "100000", "serviceMapping": "LONGITUDE" }, 
         *   { "valueType": "LONG, "multiplier": "1", "minValue" : "0" , "serviceMapping": "TIME"  }, 
         *   { "valueType": "LONG", "multiplier": "1", "minValue" : "0" , "serviceMapping" : "COORD_SYSTEM"  } 
         * ]
         */
        for (int i=0; i<fieldsJson.length(); i++) {

            try {
                
                // { "valueType": "double", "multiplier": "100000", "serviceMapping": "latitude" }
                JSONObject fieldJson = (JSONObject)fieldsJson.getJSONObject(i);
                
                fields.add(new GeoSpatialDatatypeFieldConfiguration(fieldJson));

            } catch (JSONException e) {
                
                log.warn("Invalid JSON for field description: " + e.getMessage());
                throw new InvalidGeoSpatialDatatypeConfigurationError(e.getMessage()); // forward exception
            }
        }
        
        // validate that there is at least one field defined for the geospatial datatype
        if (fields.isEmpty()) {
            throw new InvalidGeoSpatialDatatypeConfigurationError(
                "Geospatial datatype config for datatype " + uri + " must have at least one field, but has none.");
        }
        
        // validate that there are no duplicate service mappings used for the fields
        final Set<ServiceMapping> serviceMappings = new HashSet<ServiceMapping>();
        final Set<String> customServiceMappings = new HashSet<String>();
        for (int i=0; i<fields.size(); i++) {
            
            final GeoSpatialDatatypeFieldConfiguration field = fields.get(i);
            final ServiceMapping curServiceMapping = field.getServiceMapping();
            
            if (ServiceMapping.CUSTOM.equals(curServiceMapping)) {
                
                final String customServiceMapping = field.getCustomServiceMapping();
                
                if (customServiceMappings.contains(customServiceMapping)) {
                    throw new InvalidGeoSpatialDatatypeConfigurationError(
                        "Duplicate custom service mapping used for geospatial datatype config: " + customServiceMapping);                    
                }
                customServiceMappings.add(customServiceMapping);
                
            } else if (serviceMappings.contains(curServiceMapping)) {
                throw new InvalidGeoSpatialDatatypeConfigurationError(
                    "Duplicate service mapping used for geospatial datatype config: " + curServiceMapping);
            }
            
            serviceMappings.add(curServiceMapping);
        }
        
        initDerivedMembers();
    }
    
    /**
     * Alternative constructor (to ease writing test cases)
     * 
     * @param uri
     * @param fields
     */
    public GeoSpatialDatatypeConfiguration(
        final String uriString, final IGeoSpatialLiteralSerializer literalSerializer, 
        final List<GeoSpatialDatatypeFieldConfiguration> fields) {
        
        if (uriString==null || uriString.isEmpty()) {
            throw new InvalidGeoSpatialDatatypeConfigurationError("URI string must not be null or empty.");
        }

        if (literalSerializer==null) {
            throw new InvalidGeoSpatialDatatypeConfigurationError("Literal serializer must not be null.");
        }
        
        if (fields==null) {
            throw new InvalidGeoSpatialDatatypeConfigurationError("Fields must not be null.");
        }

        this.uri = new URIImpl(uriString);
        this.literalSerializer = literalSerializer;
        this.fields = fields;
     
        initDerivedMembers();
    }
    
    public URI getUri() {
        return uri;
    }
    
    public IGeoSpatialLiteralSerializer getLiteralSerializer() {
        return literalSerializer;
    }
    
    /**
     * @return the list of fields defining the datatype (never null)
     */
    public List<GeoSpatialDatatypeFieldConfiguration> getFields() {
        return fields;
    }
    
    /**
     * Computes the index of a field with a given (predefined) service mapping.
     * Returns -1 if no such component present in the datatype or the mapping
     * that is passed in is null.
     * 
     * @param mapping
     * @return
     */
    public int idxOfField(final ServiceMapping mapping) {
        if (mapping!=null) {
        
            for (int i=0; i<getFields().size(); i++) {
                if (mapping == fields.get(i).getServiceMapping()) {
                    return i;
                }
                
            }
        }
        
        return -1; // fallback
    }
    
    public int[] idxsOfCustomFields(final Set<String> customFields) {
        
        if (customFields==null) {
            return new int[0];
        }
        
        final int[] ret = new int[customFields.size()];
        Arrays.fill(ret, -1); // pre-initialize
        
        int ctr=0;
        for (int i=0; i<getFields().size(); i++) {
            
            final GeoSpatialDatatypeFieldConfiguration field = fields.get(i);

            for (final String customField : customFields) {
                
                if (ServiceMapping.CUSTOM == field.getServiceMapping() &&
                    customField!=null && customField.equals(field.getCustomServiceMapping())) {
                    
                    ret[ctr++] = i;
                }
            }
        }
        
        return ret;
    }
    
    /**
     * @return the number of dimensions (i.e., fields) of the configuration
     */
    public int getNumDimensions() {
        return fields.size();
    }
    
    
    public boolean hasLat() {
        return hasLat;
    }

    public boolean hasLon() {
        return hasLon;
    }

    public boolean hasTime() {
        return hasTime;
    }

    public boolean hasCoordSystem() {
        return hasCoordSystem;
    }

    public Map<String,Integer> getCustomFieldsIdxs() {
        return customFieldsIdxs; 
    }
    
    public boolean hasCustomFields() {
        return !customFieldsIdxs.keySet().isEmpty();
    }
    
    public boolean hasCustomField(final String field) {
        return customFieldsIdxs.keySet().contains(field);
    }
    
    public Integer getCustomFieldIdx(final String customField) {
        return customFieldsIdxs.get(customField);
    }
    
    public ValueType getValueTypeOfCustomField(final String customField) {
        
        final Integer idx = getCustomFieldIdx(customField);
        
        return idx==null ? null : fields.get(idx).getValueType();
        
    }
    
    /**
     * Initialized derived member variables, allowing efficient access to nested
     * information (such as the field types).
     */
    void initDerivedMembers() {

        for (int i=0; i<fields.size(); i++) {
            
            final GeoSpatialDatatypeFieldConfiguration field = fields.get(i);
            
            switch (field.getServiceMapping()) 
            {
            case LATITUDE:
                hasLat = true;
                break;
            case LONGITUDE:
                hasLon = true;
                break;
            case COORD_SYSTEM:
                hasCoordSystem = true;
                break;
            case TIME:
                hasTime = true;
                break;
            case CUSTOM:
                customFieldsIdxs.put(field.getCustomServiceMapping(), i);
                break;
            default:
                throw new InvalidGeoSpatialDatatypeConfigurationError(
                    "Unhandled field type: " + field.getServiceMapping());
            }
        }
    }

}