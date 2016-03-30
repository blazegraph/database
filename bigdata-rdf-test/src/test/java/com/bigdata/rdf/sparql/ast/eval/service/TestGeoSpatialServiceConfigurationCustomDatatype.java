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

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Data driven test suite testing configurability of GeoSpatial service.
 * The few tests in this class replicate examples from {@link TestGeoSpatialServiceConfiguration}
 * with a custom datatype instead of using the built-in datatype.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialServiceConfigurationCustomDatatype extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialServiceConfigurationCustomDatatype() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialServiceConfigurationCustomDatatype(String name) {
        super(name);
    }

    /**
     * Rectangle query over lat+lon+time datatype and database containing
     * lat+lon+time only data.
     */    
    public void testCustomDatatypeRect01LatLonTime() throws Exception {
       
       new TestHelper(
          "geo-custom-rect01-lat-lon-time",
          "geo-custom-rect01-lat-lon-time.rq", 
          "geo-custom-grid101010-lat-lon-time.nt",
          "geo-custom-rect01-lat-lon-time.srx").runTest();
       
    }
    
    /**
     * Rectangle query over lat+lon+time datatype and database containing mixed data.
     */    
    public void testCustomDatatypeRect01LatLonTimeMixedData() throws Exception {
       
       new TestHelper(
          "geo-custom-rect01-lat-lon-time-mixeddata",
          "geo-custom-rect01-lat-lon-time.rq", 
          "geo-custom-grid101010-mixed.nt",
          "geo-custom-rect01-lat-lon-time.srx").runTest();
       
    }
    
    /**
     * Rectangle query over lat+time+lon datatype and database containing
     * lat+time+lon only data.
     */    
    public void testCustomDatatypeRect01LatTimeLon() throws Exception {
       
       new TestHelper(
          "geo-custom-rect01-lat-time-lon",
          "geo-custom-rect01-lat-time-lon.rq", 
          "geo-custom-grid101010-lat-time-lon.nt",
          "geo-custom-rect01-lat-time-lon.srx").runTest();
       
    }
    
    /**
     * Rectangle query over lat+lon+time datatype and database containing mixed data.
     */    
    public void testCustomDatatypeRect01LatTimeLonMixedData() throws Exception {
       
       new TestHelper(
          "geo-custom-rect01-lat-time-lon-mixeddata",
          "geo-custom-rect01-lat-time-lon.rq", 
          "geo-custom-grid101010-mixed.nt",
          "geo-custom-rect01-lat-time-lon.srx").runTest();
       
    }    
    
    /**
     * Rectangle query over lat+time+lon datatype and database containing
     * lat+time+lon only data.
     */    
    public void testCustomDatatypeRect01TimeLatLon() throws Exception {
       
       new TestHelper(
          "geo-custom-rect01-time-lat-lon",
          "geo-custom-rect01-time-lat-lon.rq", 
          "geo-custom-grid101010-time-lat-lon.nt",
          "geo-custom-rect01-time-lat-lon.srx").runTest();
       
    }
    
    /**
     * Rectangle query over time+lat+lon datatype and database containing mixed data.
     */    
    public void testCustomDatatypeRect01TimeLatLonMixedData() throws Exception {
       
       new TestHelper(
          "geo-custom-rect01-time-lat-lon-mixeddata",
          "geo-custom-rect01-time-lat-lon.rq", 
          "geo-custom-grid101010-mixed.nt",
          "geo-custom-rect01-time-lat-lon.srx").runTest();
       
    }   
    
    /**
     * Circle query over lat+lon+time datatype and database containing
     * lat+lon+time only data.
     */    
    public void testCustomDatatypeCircle01LatLonTime() throws Exception {
       
       new TestHelper(
          "geo-custom-circle01-lat-lon-time",
          "geo-custom-circle01-lat-lon-time.rq", 
          "geo-custom-grid101010-lat-lon-time.nt",
          "geo-custom-circle01-lat-lon-time.srx").runTest();
       
    }
    
    /**
     * Circle query over lat+lon+time datatype and database containing mixed data.
     */    
    public void testCustomDatatypeCircle01LatLonTimeMixedData() throws Exception {
       
       new TestHelper(
          "geo-custom-circle01-lat-lon-time-mixeddata",
          "geo-custom-circle01-lat-lon-time.rq", 
          "geo-custom-grid101010-mixed.nt",
          "geo-custom-circle01-lat-lon-time.srx").runTest();
       
    }
    
    /**
     * Circle query over lat+time+lon datatype and database containing
     * lat+time+lon only data.
     */    
    public void testCustomDatatypeCircle01LatTimeLon() throws Exception {
       
       new TestHelper(
          "geo-custom-circle01-lat-time-lon",
          "geo-custom-circle01-lat-time-lon.rq", 
          "geo-custom-grid101010-lat-time-lon.nt",
          "geo-custom-circle01-lat-time-lon.srx").runTest();
       
    }
    
    /**
     * Rectangle query over lat+lon+time datatype and database containing mixed data.
     */    
    public void testCustomDatatypeCircle01LatTimeLonMixedData() throws Exception {
       
       new TestHelper(
          "geo-custom-circle01-lat-time-lon-mixeddata",
          "geo-custom-circle01-lat-time-lon.rq", 
          "geo-custom-grid101010-mixed.nt",
          "geo-custom-circle01-lat-time-lon.srx").runTest();
       
    }    
    
    /**
     * Circle query over lat+time+lon datatype and database containing
     * lat+time+lon only data.
     */    
    public void testCustomDatatypeCircle01TimeLatLon() throws Exception {
       
       new TestHelper(
          "geo-custom-circle01-time-lat-lon",
          "geo-custom-circle01-time-lat-lon.rq", 
          "geo-custom-grid101010-time-lat-lon.nt",
          "geo-custom-circle01-time-lat-lon.srx").runTest();
       
    }
    
    /**
     * Circle query over time+lat+lon datatype and database containing mixed data.
     */    
    public void testCustomDatatypeCircle01TimeLatLonMixedData() throws Exception {
       
       new TestHelper(
          "geo-custom-circle01-time-lat-lon-mixeddata",
          "geo-custom-circle01-time-lat-lon.rq", 
          "geo-custom-grid101010-mixed.nt",
          "geo-custom-circle01-time-lat-lon.srx").runTest();
       
    } 


    /**
     * Correct rejection test with search query for datatype that is not registered.
     * 
     * @throws Exception
     */
    public void testUnknownDatatypeRejected() throws Exception {
        
        try {
            new TestHelper(
               "geo-custom-invalid-dt",
               "geo-custom-invalid-dt.rq", 
               "geo-custom-grid101010-mixed.nt",
               "geo-circle0203-custom-dt.srx").runTest();
        } catch (Exception e) {
            return; // expected
        }
        
        throw new RuntimeException("Expected to run into exception.");
        
     }


    /**
     * Verify precision handling, which is changed in the configuration to be 6 for
     * the first component and 5 for the second one. See 
     * {@link TestGeoSpatialServiceEvaluation#testInRectangleQuery08()} for baseline.
     * 
     * The query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * SELECT ?res ?o WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "1.10#1.100000001111" .
     *     ?res geo:spatialRectangleNorthEast "6.666666#7" .
     *     ?res geo:timeStart "0" .
     *     ?res geo:timeEnd "0" .
     *   }
     *   ?res ?p ?o .
     * }
     * 
     * is evaluated over data
     * 
     * <http://s0> <http://p> "0#0#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s1> <http://p> "1.1#1.1#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s2> <http://p> "2.22#2.22#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s3> <http://p> "3.333#3.333#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s4> <http://p> "4.4444#4.4444#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s5> <http://p> "5.55555#5.55555#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6> <http://p> "6.666666#6.6666666#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6b> <http://p> "6.66667#6.6666666#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6c> <http://p> "6.66666#6.66667#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6c> <http://p> "6.66667#6.66667#0"^^<http://my.custom.datatype/lat-lon-time> .
     * 
     * With the given precision, the query is equivalent to
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * SELECT ?res ?o WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "1.10#1.1" .
     *     ?res geo:spatialRectangleNorthEast "6.666666#7" .
     *     ?res geo:timeStart "0" .
     *     ?res geo:timeEnd "0" .
     *   }
     *   ?res ?p ?o .
     * }
     * 
     * and the data is equivalent to 
     * 
     * <http://s0> <http://p> "0#0#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s1> <http://p> "1.1#1.1#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s2> <http://p> "2.22#2.22#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s3> <http://p> "3.333#3.333#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s4> <http://p> "4.4444#4.4444#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s5> <http://p> "5.55555#5.55555#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6> <http://p> "6.666666#6.666666#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6b> <http://p> "6.66667#6.666666#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6c> <http://p> "6.66666#6.66667#0"^^<http://my.custom.datatype/lat-lon-time> .
     * <http://s6d> <http://p> "6.66667#6.66667#0"^^<http://my.custom.datatype/lat-lon-time> .
     *  
     * Consequently, only subjects s0 and s6a, s6b, and s6d are *not* contained in the
     * result, while all others are.
     */
    public void testCustomDatatypePrecision() throws Exception {
       
       new TestHelper(
          "geo-custom-precisiontest",
          "geo-custom-precisiontest.rq", 
          "geo-custom-precisiontest.nt",
          "geo-custom-precisiontest.srx").runTest();
       
    }

    
    public void testCoordinateSystemInLatLonDatatype() throws Exception {
       
       new TestHelper(
          "geo-coordSystem2-llc01",
          "geo-coordSystem2-llc01.rq", 
          "geo-coordSystem2.nt",
          "geo-coordSystem2-llc01.srx").runTest();
       
    }

    public void testCoordinateSystemInNonLatLonDatatype() throws Exception {
        
        new TestHelper(
           "geo-coordSystem2-tc01",
           "geo-coordSystem2-tc01.rq", 
           "geo-coordSystem2.nt",
           "geo-coordSystem2-tc01.srx").runTest();
        
     }

    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "false");

        // TM not available with quads.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        // enable GeoSpatial index
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL, "true");

        // set GeoSpatial configuration: use a higher precision and range shifts; 
        // the test accounts for this higher precision (and assert that range shifts
        // actually do not harm the evaluation process)
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".0",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/lat-lon-time\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\" : \"TIME\"  } "
           + "]}}");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".1",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/time-lat-lon\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\" : \"TIME\"  }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" } "
           + "]}}");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".2",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/lat-time-lon\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\" : \"TIME\"  }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" } "
           + "]}}");
        

        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".3",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/lat-lon-coord\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"COORD_SYSTEM\" } "
           + "]}}");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".4",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/time-coord\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"serviceMapping\" : \"TIME\"  }, "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"COORD_SYSTEM\" } "
           + "]}}");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.VOCABULARY_CLASS,
           "com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestVocabulary");
        
        return properties;

    }
}
