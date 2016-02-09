package com.bigdata.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription;

public class GeoSpatialDatatypeConfig {
    
    final static private Logger log = Logger.getLogger(GeoSpatialDatatypeConfig.class);
    
    private final String uri;
    
    private List<SchemaFieldDescription> fields;

    public GeoSpatialDatatypeConfig(String uri, JSONArray fieldsJson) {
        
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
