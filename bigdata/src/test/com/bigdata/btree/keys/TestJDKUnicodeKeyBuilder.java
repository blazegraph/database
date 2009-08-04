/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Nov 29, 2007
 */

package com.bigdata.btree.keys;

import java.util.Properties;

import com.bigdata.btree.AbstractUnicodeKeyBuilderTestCase;
import com.bigdata.btree.keys.KeyBuilder.Options;

/**
 * 
 * @todo test factory methods for configuring various parameters.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJDKUnicodeKeyBuilder extends AbstractUnicodeKeyBuilderTestCase {

    /**
     * 
     */
    public TestJDKUnicodeKeyBuilder() {
    }

    /**
     * @param arg0
     */
    public TestJDKUnicodeKeyBuilder(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {

        Properties properties = new Properties(super.getProperties());
        
        properties.setProperty(Options.COLLATOR,CollatorEnum.JDK.toString());
        
        return properties;
        
    }
    
    public void test_correctCollator() {
        
        Properties properties = getProperties();
        
        log.info("properties="+properties);
        
        KeyBuilder keyBuilder = (KeyBuilder) KeyBuilder
                .newUnicodeInstance(properties);

        assertEquals(JDKSortKeyGenerator.class, keyBuilder
                .getSortKeyGenerator().getClass());
        
    }
    
}
