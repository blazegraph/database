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
 * Created on February 22, 2016
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.ArrayList;
import java.util.List;

import com.bigdata.rdf.internal.impl.extensions.InvalidGeoSpatialDatatypeConfigurationError;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.service.geospatial.GeoSpatialConfig;
import com.bigdata.service.geospatial.GeoSpatialDatatypeConfiguration;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration.ServiceMapping;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration.ValueType;

/**
 * Test suite testing the form-JSON parsing facilities for the GeoSpatial service configuration.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialServiceConfigurationParsing extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialServiceConfigurationParsing() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialServiceConfigurationParsing(String name) {
        super(name);
    }

    public void testAcceptSingleDatatypeConfig() {
        
        final String config1Uri = "http://my.custom.datatype1.uri";
        
        final String config1 = sampleConfigComplete(config1Uri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config1);
        
        final GeoSpatialConfig conf = new GeoSpatialConfig(datatypeConfigs, "http://my.custom.datatype1.uri");
        
        final List<GeoSpatialDatatypeConfiguration> parsedDatatypeConfigs = conf.getDatatypeConfigs();
        assertEquals(parsedDatatypeConfigs.size(),1);
        
        final GeoSpatialDatatypeConfiguration parsedDatatypeConfig = parsedDatatypeConfigs.get(0);
        assertEquals(parsedDatatypeConfig.getUri().stringValue(), config1Uri);
        
        final List<GeoSpatialDatatypeFieldConfiguration> fields = parsedDatatypeConfig.getFields();
        assertEquals(fields.size(), 5);
       
        // field 0 validation
        final GeoSpatialDatatypeFieldConfiguration field0 = fields.get(0);
        assertEquals(field0.getValueType(), ValueType.DOUBLE);
        assertEquals(field0.getMultiplier(), 100000);
        assertEquals(field0.getMinValue(), null);
        assertEquals(field0.getServiceMapping(), ServiceMapping.LATITUDE);
        assertEquals(field0.getCustomServiceMapping(), null);
        
        // field 1 validation
        final GeoSpatialDatatypeFieldConfiguration field1 = fields.get(1);
        assertEquals(field1.getValueType(), ValueType.DOUBLE);
        assertEquals(field1.getMultiplier(), 100000);
        assertEquals(field1.getMinValue(), null);
        assertEquals(field1.getServiceMapping(), ServiceMapping.LONGITUDE);
        assertEquals(field1.getCustomServiceMapping(), null);
        
        // field 2 validation
        final GeoSpatialDatatypeFieldConfiguration field2 = fields.get(2);
        assertEquals(field2.getValueType(), ValueType.LONG);
        assertEquals(field2.getMultiplier(), 1);
        assertEquals(field2.getMinValue(), Long.valueOf(0));
        assertEquals(field2.getServiceMapping(), ServiceMapping.TIME);
        assertEquals(field2.getCustomServiceMapping(), null);

        // field 3 validation
        final GeoSpatialDatatypeFieldConfiguration field3 = fields.get(3);
        assertEquals(field3.getValueType(), ValueType.LONG);
        assertEquals(field3.getMultiplier(), 1);
        assertEquals(field3.getMinValue(), Long.valueOf(1));
        assertEquals(field3.getServiceMapping(), ServiceMapping.COORD_SYSTEM);
        assertEquals(field3.getCustomServiceMapping(), null);

        // field 4 validation
        final GeoSpatialDatatypeFieldConfiguration field4 = fields.get(4);
        assertEquals(field4.getValueType(), ValueType.DOUBLE);
        assertEquals(field4.getMultiplier(), 5);
        assertEquals(field4.getMinValue(), Long.valueOf(2));
        assertEquals(field4.getServiceMapping(), ServiceMapping.CUSTOM);
        assertEquals(field4.getCustomServiceMapping(), "myCustomMappingStr");
        
    }
    
    public void testAcceptMultipleDatatypeConfigs() {
        
        final String config1Uri = "http://my.custom.datatype1.uri";
        final String config2Uri = "http://my.custom.datatype2.uri";
        final String config3Uri = "http://my.custom.datatype3.uri";
        
        /**
         * Three times exactly the same configuration, just different URIs
         */
        final String config1 = sampleConfigComplete(config1Uri);
        final String config2 = sampleConfigComplete(config2Uri);
        final String config3 = sampleConfigComplete(config3Uri);

        // parse config
        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config1);
        datatypeConfigs.add(config2);
        datatypeConfigs.add(config3);
        
        final GeoSpatialConfig conf = new GeoSpatialConfig(datatypeConfigs, config1Uri);
        
        // run tests
        final List<GeoSpatialDatatypeConfiguration> parsedDatatypeConfigs = conf.getDatatypeConfigs();
        assertEquals(parsedDatatypeConfigs.size(),3);

        for (int i=0; i<parsedDatatypeConfigs.size(); i++) {
            
            final GeoSpatialDatatypeConfiguration parsedDatatypeConfig = parsedDatatypeConfigs.get(i);
            
            if (i==0) {
                assertEquals(parsedDatatypeConfig.getUri().stringValue(), config1Uri);
            } else if (i==1) {
                assertEquals(parsedDatatypeConfig.getUri().stringValue(), config2Uri);
            } else if (i==2) {
                assertEquals(parsedDatatypeConfig.getUri().stringValue(), config3Uri);
            }
            
            final List<GeoSpatialDatatypeFieldConfiguration> fields = parsedDatatypeConfig.getFields();
            assertEquals(fields.size(), 5);
           
            // field 0 validation
            final GeoSpatialDatatypeFieldConfiguration field0 = fields.get(0);
            assertEquals(field0.getValueType(), ValueType.DOUBLE);
            assertEquals(field0.getMultiplier(), 100000);
            assertEquals(field0.getMinValue(), null);
            assertEquals(field0.getServiceMapping(), ServiceMapping.LATITUDE);
            assertEquals(field0.getCustomServiceMapping(), null);
            
            // field 1 validation
            final GeoSpatialDatatypeFieldConfiguration field1 = fields.get(1);
            assertEquals(field1.getValueType(), ValueType.DOUBLE);
            assertEquals(field1.getMultiplier(), 100000);
            assertEquals(field1.getMinValue(), null);
            assertEquals(field1.getServiceMapping(), ServiceMapping.LONGITUDE);
            assertEquals(field1.getCustomServiceMapping(), null);
            
            // field 2 validation
            final GeoSpatialDatatypeFieldConfiguration field2 = fields.get(2);
            assertEquals(field2.getValueType(), ValueType.LONG);
            assertEquals(field2.getMultiplier(), 1);
            assertEquals(field2.getMinValue(), Long.valueOf(0));
            assertEquals(field2.getServiceMapping(), ServiceMapping.TIME);
            assertEquals(field2.getCustomServiceMapping(), null);
    
            // field 3 validation
            final GeoSpatialDatatypeFieldConfiguration field3 = fields.get(3);
            assertEquals(field3.getValueType(), ValueType.LONG);
            assertEquals(field3.getMultiplier(), 1);
            assertEquals(field3.getMinValue(), Long.valueOf(1));
            assertEquals(field3.getServiceMapping(), ServiceMapping.COORD_SYSTEM);
            assertEquals(field3.getCustomServiceMapping(), null);
    
            // field 4 validation
            final GeoSpatialDatatypeFieldConfiguration field4 = fields.get(4);
            assertEquals(field4.getValueType(), ValueType.DOUBLE);
            assertEquals(field4.getMultiplier(), 5);
            assertEquals(field4.getMinValue(), Long.valueOf(2));
            assertEquals(field4.getServiceMapping(), ServiceMapping.CUSTOM);
            assertEquals(field4.getCustomServiceMapping(), "myCustomMappingStr");
            
        }        
    }
    
    
    public void testRejectConfigWithNoFields() {
        
        final String config1Uri = "http://my.custom.datatype1.uri";
        
        final String config1 = sampleConfigNoFields(config1Uri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config1);
        
        try {
            new GeoSpatialConfig(datatypeConfigs, config1Uri);
        } catch (InvalidGeoSpatialDatatypeConfigurationError e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");
        
    }
    
    public void testRejectSyntacticallyInvalidConfig() {
        
        final String config1Uri = "http://my.custom.datatype1.uri";
        
        final String config1 = sampleConfigSyntacticallyInvalid(config1Uri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config1);
        
        
        try {
            new GeoSpatialConfig(datatypeConfigs, config1Uri);
        } catch (IllegalArgumentException e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");
        
    }

    public void testRejectMissingDatatypeConfiguration() {
        
        final String configUri = "http://my.custom.datatype1.uri";
        
        final String config = sampleConfigWithMissingValueType(configUri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config);
        
        try {
            new GeoSpatialConfig(datatypeConfigs, configUri);
        } catch (InvalidGeoSpatialDatatypeConfigurationError e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");

    }
    
    public void testRejectMissingServiceMappingConfiguration() {
        
        final String configUri = "http://my.custom.datatype1.uri";
        
        final String config = sampleConfigWithMissingServiceMapping(configUri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config);
        
        
        try {
            new GeoSpatialConfig(datatypeConfigs, configUri);
        } catch (InvalidGeoSpatialDatatypeConfigurationError e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");

    }
    
    public void testRejectMappingConflict() {
        
        final String configUri = "http://my.custom.datatype1.uri";
        
        final String config = sampleConfigWithMappingConflict(configUri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config);
        
        
        try {
            new GeoSpatialConfig(datatypeConfigs, configUri);
        } catch (InvalidGeoSpatialDatatypeConfigurationError e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");
        
    }

    public void testRejectInvalidUri() {
        
        final String configUri = "this is not a valid uri";
        
        final String config = sampleConfigWithMappingConflict(configUri);

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config);
        
        try {
            new GeoSpatialConfig(datatypeConfigs, configUri);
        } catch (InvalidGeoSpatialDatatypeConfigurationError e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");
        
    }

    
    public void testRejectUriConflict() {

        final String configUri = "http://my.custom.datatype1.uri";
        
        final String config1 = sampleConfigWithMappingConflict(configUri);
        final String config2 = sampleConfigWithMappingConflict(configUri); // same URI -> invalid!

        final List<String> datatypeConfigs = new ArrayList<String>();
        datatypeConfigs.add(config1);
        datatypeConfigs.add(config2);
        
        try {
            new GeoSpatialConfig(datatypeConfigs, configUri);
        } catch (InvalidGeoSpatialDatatypeConfigurationError e) {
            return; // expected !
        }
        
        throw new RuntimeException("Expected to run into exception, but did not.");
    }


    /**
     * Test helper that returns a sample configuration of the given URI.
     */
    String sampleConfigComplete(String uri) {
        
        final String config = 
                "{\"config\": "
                + "{ \"uri\": \"" + uri + "\", "
                + "  \"fields\": [ "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
                + "    { \"valueType\": \"LONG\", \"multiplier\": \"1\", \"minValue\" : \"0\" , \"serviceMapping\": \"TIME\"  }, "
                + "    { \"valueType\": \"LONG\", \"multiplier\": \"1\", \"minValue\" : \"1\" , \"serviceMapping\" : \"COORD_SYSTEM\"  }, "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"5\", \"minValue\" : \"2\" , \"serviceMapping\" : \"myCustomMappingStr\"  } "            
                + "  ]"
                + " }"
                + "}";        
        
        return config;
    }
    
    /**
     * Test helper that returns a syntactically valid configuration with no fields.
     */
    String sampleConfigNoFields(String uri) {
    
        
        final String config = 
                "{\"config\": "
                + "{ \"uri\": \"" + uri + "\", "
                + "  \"fields\": [] "
                + " }"
                + "}";        
        
        return config;
    }
    
    /**
     * Test helper that returns a syntactically invalid configuration
     */
    String sampleConfigSyntacticallyInvalid(String uri) {
    
        
        final String config = 
                "{\"config\": "
                + "{ \"uri\": \"" + uri + "\", "
                + "  \"fields\": " // opening "[" bracked for field missing
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
                + "    { \"valueType\": \"LONG\", \"multiplier\": \"1\", \"minValue\" : \"0\" , \"serviceMapping\": \"TIME\"  }, "
                + "    { \"valueType\": \"LONG\", \"multiplier\": \"1\", \"minValue\" : \"1\" , \"serviceMapping\" : \"COORD_SYSTEM\"  }, "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"5\", \"minValue\" : \"2\" , \"serviceMapping\" : \"myCustomMappingStr\"  } "            
                + "  ]"
                + " }"
                + "}";     
        
        return config;
    }
    
    /**
     * Test helper that returns a syntactically invalid configuration
     */
    String sampleConfigWithMappingConflict(String uri) {
    
        final String config = 
                "{\"config\": "
                + "{ \"uri\": \"" + uri + "\", "
                + "  \"fields\": [ "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
                + "    { \"valueType\": \"LONG\", \"multiplier\": \"1\", \"minValue\" : \"0\" , \"serviceMapping\": \"LATITUDE\"  }, " // twice latitude
                + "    { \"valueType\": \"LONG\", \"multiplier\": \"1\", \"minValue\" : \"1\" , \"serviceMapping\" : \"COORD_SYSTEM\"  }, "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"5\", \"minValue\" : \"2\" , \"serviceMapping\" : \"myCustomMappingStr\"  } "            
                + "  ]"
                + " }"
                + "}";        
        
        return config;
    }
    
    /**
     * Test helper that returns a syntactically invalid configuration
     */
    String sampleConfigWithMissingValueType(String uri) {
    
        
        final String config = 
                "{\"config\": "
                + "{ \"uri\": \"" + uri + "\", "
                + "  \"fields\": [ "
                + "    { \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" } "
                + "  ]"
                + " }"
                + "}";        
        
        return config;
    }
    
    /**
     * Test helper that returns a syntactically invalid configuration
     */
    String sampleConfigWithMissingServiceMapping(String uri) {
    
        
        final String config = 
                "{\"config\": "
                + "{ \"uri\": \"" + uri + "\", "
                + "  \"fields\": [ "
                + "    { \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\" } "
                + "  ]"
                + " }"
                + "}";        
        
        return config;
    }
    
}
