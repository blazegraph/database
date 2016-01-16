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
 * Created on Sep 18, 2015
 */
package com.bigdata.service;

import java.util.ArrayList;
import java.util.List;

import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription.Datatype;

/**
 * Singleton class providing access to the GeoSpatial index configuration.
 * This is thought as a workaround that eases configuration, we should
 * probably get rid of this class once we have fully generalized the system
 * to deal with arbitrary indices.
 * 
 * @author msc
 */
public class GeoSpatialConfig {

   private static GeoSpatialConfig instance;
   
   private static final String COMPONENT_SEPARATOR = ";";
   private static final String FIELD_SEPARATOR = "#";
   
   private SchemaDescription schemaDescription;
   
   private GeoSpatialConfig() {
      
      init(null);
   }
   
   public static GeoSpatialConfig getInstance() {
      
      if (instance==null) {
         instance = new GeoSpatialConfig();
      }
      
      return instance;
   }
   
   public void init(String initStringSchemaDescription) {
      
      initSchemaDescription(initStringSchemaDescription);
   }
   
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
   private SchemaDescription defaultSchemaDescription() {
      
      final List<SchemaFieldDescription> sfd = 
            new ArrayList<SchemaFieldDescription>();

      sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* latitude */
      sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* longitude */
      sfd.add(new SchemaFieldDescription(Datatype.LONG, 1));  /* time */
      
      return new SchemaDescription(sfd);
   }

}
