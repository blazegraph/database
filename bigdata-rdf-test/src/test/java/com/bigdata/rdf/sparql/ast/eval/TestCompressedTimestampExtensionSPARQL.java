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
 * Created on Nov 12, 2015
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Properties;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.store.AbstractTripleStore;


/**
 * Data driven test suite {@link CompressedTimestampExtension}, testing
 * real SPARQL queries including mathematical operations over the extension.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestCompressedTimestampExtensionSPARQL extends AbstractDataDrivenSPARQLTestCase {


    public TestCompressedTimestampExtensionSPARQL() {
    }

    /**
     * @param name
     */
    public TestCompressedTimestampExtensionSPARQL(String name) {
        super(name);
    }

    /**
     * Simple SELECT query returning data typed with the given timestamp.
     */
    public void test_compressed_timestamp_01a() throws Exception {

        new TestHelper(
            "compressed-timestamp-01a", // testURI,
            "compressed-timestamp-01a.rq",// queryFileURL
            "compressed-timestamp.ttl",// dataFileURL
            "compressed-timestamp-01.srx"// resultFileURL
        ).runTest();

    }
    
    /**
     * Simple SELECT query returning data typed with the given timestamp,
     * where we have several FILTERs that should evaluate to true.
     */
    public void test_compressed_timestamp_01b() throws Exception {

        new TestHelper(
            "compressed-timestamp-01b", // testURI,
            "compressed-timestamp-01b.rq",// queryFileURL
            "compressed-timestamp.ttl",// dataFileURL
            "compressed-timestamp-01.srx"// resultFileURL
        ).runTest();

    }
    
    /**
     * Simple SELECT query returning data typed with the given timestamp,
     * where we have several FILTERs that should evaluate to true.
     */
    public void test_compressed_timestamp_01c() throws Exception {

        new TestHelper(
            "compressed-timestamp-01c", // testURI,
            "compressed-timestamp-01c.rq",// queryFileURL
            "compressed-timestamp.ttl",// dataFileURL
            "compressed-timestamp-01.srx"// resultFileURL
        ).runTest();

    }
    
    /**
     * Simple SELECT query returning data typed with the given timestamp,
     * where we have several FILTERs that should evaluate to true.
     */
    public void test_compressed_timestamp_01d() throws Exception {

        new TestHelper(
            "compressed-timestamp-01d", // testURI,
            "compressed-timestamp-01d.rq",// queryFileURL
            "compressed-timestamp.ttl",// dataFileURL
            "compressed-timestamp-01.srx"// resultFileURL
        ).runTest();

    }

    /**
     * SELECT query including plus and minus operations on the
     * compressed timestamp datatype
     */
    public void test_compressed_timestamp_02a() throws Exception {

        new TestHelper(
            "compressed-timestamp-02a", // testURI,
            "compressed-timestamp-02a.rq",// queryFileURL
            "compressed-timestamp.ttl",// dataFileURL
            "compressed-timestamp-02.srx"// resultFileURL
        ).runTest();

    }
    
    /**
     * SELECT query including plus and minus operations on the
     * compressed timestamp datatype plus filters with comparison
     * and mathematical operations.
     */
    public void test_compressed_timestamp_02b() throws Exception {

        new TestHelper(
            "compressed-timestamp-02b", // testURI,
            "compressed-timestamp-02b.rq",// queryFileURL
            "compressed-timestamp.ttl",// dataFileURL
            "compressed-timestamp-02.srx"// resultFileURL
        ).runTest();

    }
    
    /**
     * Hook in the compressed timestamp extension factory.
     */
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(
            AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, 
            "com.bigdata.rdf.internal.CompressedTimestampExtensionFactory");

        return properties;

    }    
}
