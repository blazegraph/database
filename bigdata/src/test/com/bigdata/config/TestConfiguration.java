/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Nov 23, 2008
 */

package com.bigdata.config;

import java.util.Properties;

import junit.framework.TestCase;

import com.bigdata.journal.IIndexManager;

/**
 * Unit tests for {@link Configuration}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConfiguration extends TestCase {

    public TestConfiguration() {

        super();
        
    }
    
    public TestConfiguration(String name) {
        
        super(name);
        
    }

    /**
     * Unit test for the override of the default by specifying a global value
     * for a property.
     */
    public void testGlobalOverride() {

        final IIndexManager indexManager = null;

        final Properties properties = new Properties();

        final String namespace = "foo.bar";

        // global property name.
        final String globalName = "bigdata.bar";
        
        final String defaultValue = "goo";
        
        final String globalOverride = "boo";
        
        assertEquals(defaultValue, Configuration.getProperty(indexManager,
                properties, namespace, globalName, defaultValue));
        
        properties.setProperty(globalName, globalOverride);
        
        assertEquals(globalOverride, Configuration.getProperty(indexManager,
                properties, namespace, globalName, defaultValue));

    }
    
    /**
     * Unit test for override of a property value specified for the exact
     * namespace (rather than some namespace prefix).
     */
    public void test_exactNamespaceOverride() {

        final IIndexManager indexManager = null;

        final Properties properties = new Properties();

        final String namespace = "foo.baz";
        
//        // local property name.
//        final String localName = "bar";

        // global property name.
        final String globalName = "bigdata.bar";
        
        final String defaultValue = "goo";
        
        final String overrideValue = "boo";
        
        assertEquals(defaultValue, Configuration.getProperty(indexManager,
                properties, namespace, globalName, defaultValue));
        
        final String overrideName = Configuration.getOverrideProperty(
                namespace, globalName);

        properties.setProperty(overrideName, overrideValue);

        assertEquals(overrideValue, Configuration.getProperty(indexManager,
                properties, namespace, globalName, defaultValue));
        
    }

    /**
     * Unit test where the property override is applied at the parent level in
     * the namespace ("foo" vs "foo.baz").
     */
    public void test_prefixNamespaceOverride() {
        
        final IIndexManager indexManager = null;

        final Properties properties = new Properties();

        final String namespace = "foo.baz";

//        // local property name.
//        final String localName = "bar";

        // global property name.
        final String globalName = "bigdata.bar";

        final String defaultValue = "goo";

        final String overrideName = Configuration.getOverrideProperty(
                namespace, globalName);
        
        final String overrideValue = "boo";

        assertEquals(defaultValue, Configuration.getProperty(indexManager,
                properties, namespace, globalName, defaultValue));

        properties.setProperty(overrideName, overrideValue);

        assertEquals(overrideValue, Configuration.getProperty(indexManager,
                properties, namespace, globalName, defaultValue));
        
    }
    
}
