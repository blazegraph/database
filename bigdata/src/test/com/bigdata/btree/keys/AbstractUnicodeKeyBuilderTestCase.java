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

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.btree.keys.KeyBuilder.Options;

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

    /**
     * Test ability to encode unicode data into a variable length byte[] that
     * allows direct byte-by-byte comparisons which maintain the local-specific
     * sort order of the original strings.
     */
    public void test_keyBuilder_unicode_string_key_us_primary() {

        /*
         * Setup an instance for US English with strength := PRIMARY.
         */

        final Properties properties = getProperties();
        
        properties.setProperty(Options.USER_LANGUAGE, Locale.US.getLanguage());

        properties.setProperty(Options.USER_COUNTRY, Locale.US.getCountry());
        
        properties.setProperty(Options.STRENGTH, StrengthEnum.Primary
                .toString());

        final DefaultKeyBuilderFactory factory = new DefaultKeyBuilderFactory(
                properties);

        assertEquals(Locale.US.getLanguage(), factory.getLocale().getLanguage());

        assertEquals(Locale.US.getCountry(), factory.getLocale().getCountry());

        assertEquals(StrengthEnum.Primary, factory.getStrength());
        
        final IKeyBuilder keyBuilder = factory.getKeyBuilder();

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

        final Properties properties = new Properties();
        
        properties.setProperty(Options.USER_LANGUAGE, Locale.US.getLanguage());

        properties.setProperty(Options.USER_COUNTRY, Locale.US.getCountry());
        
        properties.setProperty(Options.STRENGTH, ""+StrengthEnum.Identical);

        final IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance(properties);

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

        final IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance(getProperties());
        
        keyBuilder.append("Hello World!");
        
        final byte[] key = keyBuilder.getKey();
        
        assertNotSame("Not expecting a trailing nul byte.", (byte) 0,
                key[key.length - 1]);
        
    }

    /*
     * Note: This test has been disabled.  The SparseRowStore no longer makes
     * this assumption in order to be compatible with the JDK collator.
     */
//    /**
//     * Test verifies that the <code>nul</code> byte is not part of the key when
//     * a unicode string is appended to an {@link IKeyBuilder}.
//     * <p>
//     * Note: The {@link SparseRowStore} assumes that Unicode sort keys do not
//     * contain embedded <code>nul</code>s.
//     */
//    public void test_keyBuilder_unicode_String_noEmbeddedNuls() {
//
//        final IKeyBuilder keyBuilder = KeyBuilder
//                .newUnicodeInstance(getProperties());
//
//        keyBuilder.append("Hello World!");
//
//        final byte[] key = keyBuilder.getKey();
//
//        for (int i = 0; i < key.length; i++) {
//            if (key[i] == 0)
//                fail("Embedded nuls: key=" + BytesUtil.toString(key));
//        }
//        
//    }

    /**
     * Test of the ability to normalize trailing pad characters.
     */
    public void test_keyBuilder_normalizeTrailingPadCharacters() {
        
        final KeyBuilder keyBuilder = (KeyBuilder) KeyBuilder
                .newUnicodeInstance(getProperties());
    
        assertEquals(//
                keyBuilder.normalizeText(""),//
                keyBuilder.normalizeText(" ")//
                );
        assertEquals(//
                keyBuilder.normalizeText(""),//
                keyBuilder.normalizeText("  ")//
                );
        assertEquals(//
                keyBuilder.normalizeText(""),//
                keyBuilder.normalizeText("      ")//
                );
        assertEquals(//
                keyBuilder.normalizeText(" "),//
                keyBuilder.normalizeText("      ")//
                );
        assertEquals(//
                keyBuilder.normalizeText("abc"),//
                keyBuilder.normalizeText("abc      ")//
                );
        assertEquals(//
                keyBuilder.normalizeText("   abc"),//
                keyBuilder.normalizeText("   abc      ")//
                );
        assertNotSame(//
                keyBuilder.normalizeText("abc"),//
                keyBuilder.normalizeText("   abc      ")//
                );
        
    }
    
    /**
     * Test verifies that very long strings are truncated.
     * 
     * @todo verify that trailing whitespace is removed after truncation rather
     *       than before truncation.
     */
    public void test_keyBuilder_normalizeTruncatesVeryLongStrings() {

        final KeyBuilder keyBuilder = (KeyBuilder)KeyBuilder.newUnicodeInstance(getProperties());

        final String text = TestKeyBuilder.getMaximumLengthText();

        assertEquals(//
                keyBuilder.normalizeText(text),//
                keyBuilder.normalizeText(text+"abc")//
                );
        
    }
    
    /**
     * Test verifies the order among unicode sort keys, including verifying that
     * the pad byte causes a prefix such as "bro" to sort before a term which
     * extends that prefix, such as "brown".
     */
    public void test_keyBuilder_unicode_order() {        

        final KeyBuilder keyBuilder = (KeyBuilder) KeyBuilder.newUnicodeInstance(getProperties());
        
        final KVO<String>[] a = new KVO[] {
          
                new KVO<String>(keyBuilder.asSortKey("bro"),null,"bro"),
                new KVO<String>(keyBuilder.asSortKey("brown"),null,"brown"),
                new KVO<String>(keyBuilder.asSortKey("bre"),null,"bre"),
                new KVO<String>(keyBuilder.asSortKey("break"),null,"break"),
                
        };
        
        // sort by the assigned sort keys.
        Arrays.sort(a);
        
        /*
         * verify that "bre(ak)" is before "bro(wn)" and that "bre" is before
         * "break" and "bro" is before "brown".
         */
        assertEquals("bre", a[0].obj);
        assertEquals("break", a[1].obj);
        assertEquals("bro", a[2].obj);
        assertEquals("brown", a[3].obj);
        
    }
    
    /**
     * <p>
     * Test that lexiographic order is maintain when a variable length Unicode
     * field is followed by another field. This test works by comparing the
     * original multi-field key with the multi-field key formed from the
     * successor of the Unicode field followed by the other field:
     * </p>
     * 
     * <pre>
     *   
     *   [text][nextValue] LT [successor(text)][nextValue]
     *   
     * </pre>
     */
    public void test_keyBuilder_multiField_unicode() {

        final KeyBuilder keyBuilder = (KeyBuilder) KeyBuilder
                .newUnicodeInstance(getProperties());

        TestKeyBuilder.doMultiFieldTests(true/* unicode */, keyBuilder);

        /*
         * Now test some strings that contain code points outside of the 8-bit
         * range.
         */
        
//        final KeyBuilder keyBuilder = (KeyBuilder) KeyBuilder
//                .newUnicodeInstance();

        final boolean unicode = true;
        {
            
            // Note: This is "Japanese" in kanji.
            String text = "\u65E5\u672C\u8A9E / \u306B\u307B\u3093\u3054";
            
            TestKeyBuilder.doMultiFieldTest(keyBuilder, unicode, text, (byte) 0);
            TestKeyBuilder.doMultiFieldTest(keyBuilder, unicode, text, (byte) 1);
            TestKeyBuilder.doMultiFieldTest(keyBuilder, unicode, text, (byte) -1);
            TestKeyBuilder.doMultiFieldTest(keyBuilder, unicode, text, Byte.MIN_VALUE);
            TestKeyBuilder.doMultiFieldTest(keyBuilder, unicode, text, Byte.MAX_VALUE);
        }

    }
    
}
