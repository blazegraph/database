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

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;

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
    
    public void test_getOverrideProperty() {
        
        final String namespace = "U8000";
        final String namespace2 = "U100";

        final String propertyName = IndexMetadata.Options.SCATTER_SPLIT_ENABLED;

        final String overrideName = Configuration.getOverrideProperty(namespace
                + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                + LexiconKeyOrder.TERM2ID, propertyName);

        System.err.println(overrideName);

        final String defaultValue = "true";
        
        final Properties p = new Properties();

        // override this property.
        p.setProperty(overrideName, "false");

        final IIndexManager indexManager = null;

        /*
         * Verify override used for U8000.lex.TERM2ID (this is the specific case
         * for the override).
         */
        assertEquals("false", Configuration.getProperty(indexManager, p,
                namespace + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.TERM2ID, propertyName, defaultValue));

        /*
         * Verify override ignored for U8000.lex.ID2TERM (another index in the
         * same relation).
         */
        assertEquals(defaultValue, Configuration.getProperty(indexManager, p,
                namespace + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.ID2TERM, propertyName, defaultValue));

        /*
         * Verify override ignored for U100.lex.TERM2ID (an index in a different
         * relation).
         */
        assertEquals(defaultValue, Configuration.getProperty(indexManager, p,
                namespace2 + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.TERM2ID, propertyName, defaultValue));

    }

    public void test_getOverrideProperty2() {
        
        final String namespace = "U8000";
        final String namespace1 = "U100";
        final String namespace2 = "U50";

        final String propertyName = IndexMetadata.Options.SCATTER_SPLIT_DATA_SERVICE_COUNT;

        // override of a specific index in a specific relation.
        final String overrideName = Configuration.getOverrideProperty(namespace
                + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                + LexiconKeyOrder.TERM2ID, propertyName);

        // override of all indices in a different relation.
        final String overrideName2 = Configuration.getOverrideProperty(
                namespace2, propertyName);

        System.err.println(overrideName);
        System.err.println(overrideName2);

        Properties p = new Properties();

        final String defaultValue = "0";
        
        final String globalOverride = "10";

        p = new Properties(p);
        
        final String otherOverride = "5";
        
        // override the global default.
        p.setProperty(propertyName, globalOverride);
        
        // a different override for the specific index.
        p.setProperty(overrideName, "2");

        // a different override for a different relation.
        p.setProperty(overrideName2, otherOverride);

        final IIndexManager indexManager = null;

        /*
         * Verify override used for U8000.lex.TERM2ID (this is the specific case
         * for the override).
         */
        assertEquals("2", Configuration.getProperty(indexManager, p,
                namespace + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.TERM2ID, propertyName, defaultValue));

        /*
         * Verify global override used for a different index in the same
         * relation.
         */
        assertEquals(globalOverride, Configuration.getProperty(indexManager, p,
                namespace + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.ID2TERM, propertyName, defaultValue));

        /*
         * Verify global override used for an index in another relation.
         */
        assertEquals(globalOverride, Configuration.getProperty(indexManager, p,
                namespace1 + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.TERM2ID, propertyName, defaultValue));

        /*
         * Verify other override used for all indices in the namespace2
         * relation.
         */
        assertEquals(otherOverride, Configuration.getProperty(indexManager, p,
                namespace2 + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.TERM2ID, propertyName, defaultValue));
        assertEquals(otherOverride, Configuration.getProperty(indexManager, p,
                namespace2 + "." + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.ID2TERM, propertyName, defaultValue));

    }

}
