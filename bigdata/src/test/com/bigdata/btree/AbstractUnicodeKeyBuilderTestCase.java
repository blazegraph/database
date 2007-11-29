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

package com.bigdata.btree;

import java.util.Locale;
import java.util.Properties;

import com.bigdata.btree.KeyBuilder.Options;
import com.bigdata.btree.KeyBuilder.StrengthEnum;

import junit.framework.TestCase2;

/**
 * Base class for the test suites that examine support for Unicode sort keys in
 * {@link KeyBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractUnicodeKeyBuilderTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractUnicodeKeyBuilderTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractUnicodeKeyBuilderTestCase(String arg0) {
        super(arg0);
    }

    public void test_keyBuilder_unicode_char_key() {
        
        fail("write test");
    }
    
    public void test_keyBuilder_unicode_chars_key() {
        
        fail("write test");
    }

    /**
     * Test ability to encode unicode data into a variable length byte[] that
     * allows direct byte-by-byte comparisons which maintain the local-specific
     * sort order of the original strings.
     */
    public void test_keyBuilder_unicode_string_key_us_primary() {

        /*
         * Setup an instance for US English with strength := PRIMARY.
         */

        Properties properties = getProperties();
        
        properties.setProperty(Options.USER_LANGUAGE, Locale.US.getLanguage());

        properties.setProperty(Options.USER_COUNTRY, Locale.US.getCountry());
        
        properties.setProperty(Options.STRENGTH, ""+StrengthEnum.Primary);

        IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance(properties);

//        // verify assumption under that configuration.
//        {
//            RuleBasedCollator usCollator = (RuleBasedCollator) Collator
//                    .getInstance(Locale.US);
//
//            usCollator.setStrength(Collator.PRIMARY);
//
//            assertEquals(0, usCollator.compare("abc", "ABC"));
//        }
            
        byte[] key1 = keyBuilder.reset().append("abc").getKey();
        byte[] key2 = keyBuilder.reset().append("ABC").getKey();
        byte[] key3 = keyBuilder.reset().append("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));

        // all are equal using PRIMARY strength.
        assertEquals(0,BytesUtil.compareBytes(key1, key2));
        assertEquals(0,BytesUtil.compareBytes(key2, key3));
        
    }

    public void test_keyBuilder_unicode_string_key_us_identical() {

        /*
         * Setup an instance for US English with strength := IDENTICAL.
         */

        Properties properties = new Properties();
        
        properties.setProperty(Options.USER_LANGUAGE, Locale.US.getLanguage());

        properties.setProperty(Options.USER_COUNTRY, Locale.US.getCountry());
        
        properties.setProperty(Options.STRENGTH, ""+StrengthEnum.Identical);

        IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance(properties);

//        // verify assumption under that configuration.
//        {
//            RuleBasedCollator usCollator = (RuleBasedCollator) Collator
//                    .getInstance(Locale.US);
//
//            usCollator.setStrength(Collator.IDENTICAL);
//
//            assertNotSame(0, usCollator.compare("abc", "ABC"));
//        }

        // IKeyBuilder keyBuilder = new UnicodeKeyBuilder(usCollator,1000);
            
        byte[] key1 = keyBuilder.reset().append("abc").getKey();
        byte[] key2 = keyBuilder.reset().append("ABC").getKey();
        byte[] key3 = keyBuilder.reset().append("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));

        // verify ordering for IDENTICAL comparison.
        assertTrue(BytesUtil.compareBytes(key1, key2)<0);
        assertTrue(BytesUtil.compareBytes(key2, key3)>0);
        
    }

    /**
     * Test verifies that the trailing <code>nul</code> byte is not part of
     * the key when a unicode string is appended to an {@link IKeyBuilder}.
     * <p>
     * Note: The trailing <code>nul</code> byte is appended by the ICU library
     * in order to have compatibility with their C library, but it is not of
     * interest for Java processing. However, note that a <code>nul</code>
     * byte MAY be used to separate components of a complex key.
     */
    public void test_keyBuilder_unicode_String_noTrailingNul() {

        IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance();
        
        keyBuilder.append("Hello World!");
        
        byte[] key = keyBuilder.getKey();
        
        assertNotSame("Not expecting a trailing nul byte.", (byte) 0,
                key[key.length - 1]);
        
    }
    
}
