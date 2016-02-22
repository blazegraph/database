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
package com.bigdata.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription.Datatype;

/**
 * Singleton class providing access to the GeoSpatial index configuration.
 * 
 * TODO: Singleton implementation might be re-considered.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialConfig {

    final static private Logger log = Logger.getLogger(GeoSpatialConfig.class);

    private final static String JSON_STR_CONFIG = "config";
    private final static String JSON_STR_URI = "uri";
    private final static String JSON_STR_FIELDS = "fields";
   

    
    private static GeoSpatialConfig instance;

    private static final String COMPONENT_SEPARATOR = ";";
    private static final String FIELD_SEPARATOR = "#";

    private SchemaDescription schemaDescription;

    private List<GeoSpatialDatatypeConfiguration> datatypeConfigs;

    private GeoSpatialConfig() {
        init(null, null);
    }

    public static GeoSpatialConfig getInstance() {

        if (instance == null) {
            instance = new GeoSpatialConfig();
        }

        return instance;
    }

    public void init(final String initStringSchemaDescription, final List<String> geoSpatialDatatypeConfigs) {

        initSchemaDescription(initStringSchemaDescription);
        initDatatypes(geoSpatialDatatypeConfigs);
    }
   
    private void initDatatypes(List<String> geoSpatialDatatypeConfigs) {
       
        datatypeConfigs = new ArrayList<GeoSpatialDatatypeConfiguration>();
       
        if (geoSpatialDatatypeConfigs==null)
            return; // nothing to be done

        /**
         * We expect a JSON config string of the following format (example):
         * 
         * {"config": { 
         *   "uri": "<http://my.custom.datatype2.uri>", 
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
                final JSONArray fields = (JSONArray)topLevelNode.get(JSON_STR_FIELDS);

                // delegate to GeoSpatialDatatypeConfiguration for construction
                datatypeConfigs.add(new GeoSpatialDatatypeConfiguration(uri, fields));
                
            } catch (JSONException e) {
                
                log.warn("Illegal JSON configuration: " + e.getMessage());
                throw new IllegalArgumentException(e); // forward exception
            }
           
            // validate that there are no duplicate URIs used for the datatypeConfigs
            final Set<String> uris = new HashSet<String>();
            for (int i=0; i<datatypeConfigs.size(); i++) {
                
                final String curUri = datatypeConfigs.get(i).getUri();
                
                if (uris.contains(curUri)) {
                    throw new IllegalArgumentException("Duplicate URI used for geospatial datatype config: " + curUri);
                }
                
                uris.add(curUri);
            }

        }
    }

   
   public List<GeoSpatialDatatypeConfiguration> getDatatypeConfigs() {
       return datatypeConfigs;
   }

    // TODO: this is legacy and should be removed at some point
    public SchemaDescription getSchemaDescription() {
          return schemaDescription;
       }

   /**
    * Initializes the schema description based on an init string.
    * A schema string such as DOUBLE#5;DOUBLE;5;0#LONG;2;0 describes
    * three dimensions:
    * 
    * - DOUBLE#100000   -> type=DOUBLE, precision=5, no range shift
    * - DOUBLE#100000#0 -> type=DOUBLE, precision=5, range shift according to min value 0
    * - LONG#1#0   -> type=LONG, precision=0 (leave value unmodified), range shift according to min value 0
    * 
    * If no schema description string is given (or the given string is empty), 
    * the default schema description is returned.
    */
    // TODO: this is legacy and should be removed at some point
   private void initSchemaDescription(String initString) {
     

      if (initString==null || initString.isEmpty()) {
   
         schemaDescription = defaultSchemaDescription();

      } else {
         
         final List<SchemaFieldDescription> sfd = 
               new ArrayList<SchemaFieldDescription>();
         
         final String[] components = initString.split(COMPONENT_SEPARATOR);
         for (int i=0; i<components.length; i++) {
            
            final String component = components[i].trim();
            final String[] fields = component.split(FIELD_SEPARATOR);

            if (fields.length<2 || fields.length>3) {
               throw new IllegalArgumentException(
                  "Invalid number of fields in component #" + i + ": " + fields.length);
            }
            
            final String field0Str = fields[0].trim();
            final String field1Str = fields[1].trim();
            final String field2Str = fields.length==3? fields[2] : null;
            
            final Datatype datatype;
            if (field0Str.equalsIgnoreCase("DOUBLE")) {
               datatype = Datatype.DOUBLE;
            } else if (field0Str.equalsIgnoreCase("LONG")) {
               datatype = Datatype.LONG;               
            } else {
               throw new IllegalArgumentException(
                     "First field must be DOUBLE or LONG, but is: " + field0Str);
            }

            final Long precision;
            try {
               precision = Long.valueOf(field1Str);
            } catch (NumberFormatException e) {
               throw new IllegalArgumentException(
                     "Second field must be an integer value, but is: " + field1Str);               
            }
            
            final Long minValue;
            if (field2Str==null) {
               minValue = null;
            } else {
               minValue = Long.valueOf(field2Str);
            }
            
            sfd.add(new SchemaFieldDescription(datatype, precision, minValue));
         }
         
         schemaDescription = new SchemaDescription(sfd);
         
      }
      

   }
   
   /**
    * The default schema is a fixed, three-dimensional datatype
    * made up of latitude, longitude and time.
    */
   // TODO: this is legacy and should be replaced at some point
   private SchemaDescription defaultSchemaDescription() {
      
      final List<SchemaFieldDescription> sfd = 
            new ArrayList<SchemaFieldDescription>();

      sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* latitude */
      sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* longitude */
      sfd.add(new SchemaFieldDescription(Datatype.LONG, 1));  /* time */
      
      return new SchemaDescription(sfd);
   }

}
