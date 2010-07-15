/**

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
 * Created on Apr 30, 2007
 */

package com.bigdata.btree.keys;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.KeyBuilder.Options;
import com.ibm.icu.text.Collator;

/**
 * Tests for Unicode support in {@link KeyBuilder}.
 * 
 * @todo test w/ and w/o the ICU integration (can be chosen at run time via
 *       concrete subclasses). Note that some tests are specific to the ICU
 *       libraries at this time.
 * 
 * @todo write performance test for encoding strings, possibly in the context of
 *       parsed rdf data, and see if there are any easy wins in how the encoding
 *       to a sort key is handled or in alignment of the apis.
 * 
 * @todo compare performance of the ICU and JDK libraries in some application
 *       contexts. compare performance of the JNI ICU library as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestICUUnicodeKeyBuilder extends AbstractUnicodeKeyBuilderTestCase {

    /**
     * 
     */
    public TestICUUnicodeKeyBuilder() {
    }

    /**
     * @param arg0
     */
    public TestICUUnicodeKeyBuilder(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());
        
        properties.setProperty(Options.COLLATOR,CollatorEnum.ICU.toString());
        
        return properties;
        
    }
    
    public void test_correctCollator() {
        
        final Properties properties = getProperties();

        if (log.isInfoEnabled())
            log.info("properties=" + properties);

        final KeyBuilder keyBuilder = (KeyBuilder) KeyBuilder
                .newUnicodeInstance(properties);

        assertEquals(ICUSortKeyGenerator.class, keyBuilder
                .getSortKeyGenerator().getClass());
        
    }
    
    /**
     * Test examines the behavior when the
     * {@link SuccessorUtil#successor(String)} of an Unicode string is formed by
     * appending a <code>nul</code> character and reports an error if the
     * resulting byte[] when the key are formed compares as equal to the
     * original string from which the successor was formed.
     * <p>
     * Note: Since {@link Collator#IDENTICAL} appears to be required to
     * differentiate a trailing nul character (i.e., the successor of some
     * Unicode string), then I would strongly recommend that you form the sort
     * key first and then its successor (by appending a trailing nul).
     */
    public void test_keyBuilder_unicode_trailingNuls() {

        /*
         * Setup for US English.
         */

        final Properties properties = new Properties();
        
        properties.setProperty(Options.USER_LANGUAGE, Locale.US.getLanguage());

        properties.setProperty(Options.USER_COUNTRY, Locale.US.getCountry());
        
        final int[] strengths = new int[] { 
                Collator.PRIMARY,
                Collator.SECONDARY,
                Collator.TERTIARY,
                Collator.QUATERNARY,
                Collator.IDENTICAL,
                };
        
        int minStrength = -1;
        
        for(int i=0; i<strengths.length; i++) {
           
            final int strength = strengths[i];

            // set the strength on the collator.
            properties.setProperty(Options.STRENGTH, ""+Collator.IDENTICAL);
            
//            RuleBasedCollator collator = (RuleBasedCollator) Collator
//                    .getInstance(Locale.getDefault());
//            
//            collator.setStrength(strength);
            
            if(!doSuccessorTest( "Hello World!", properties)) {
                
                log.warn("Collator does not differentiate trailing nul characters at strength="+strength);
                
            } else {
                
                minStrength = strength;
                
            }

        }
        
        assertFalse(
                "Collator will not differentiate trailing nul characters at any strength.",
                minStrength == -1); 

        System.err
                .println("Minimum strength ("+minStrength+") to differentiate trailing nul character is: "
                        + (minStrength == Collator.PRIMARY ? "PRIMARY"
                                : (minStrength == Collator.SECONDARY ? "SECONDARY"
                                        : (minStrength == Collator.TERTIARY ? "TERTIARY"
                                                : (minStrength == Collator.QUATERNARY ? "QUARERNARY"
                                                        : (minStrength == Collator.IDENTICAL ? "IDENTICAL"
                                                                : ""
                                                                        + minStrength))))));
        
    }

    /**
     * Test whether or not the {@link Collator} will differentiate Unicode
     * strings that differ only in a trailing <code>nul</code> character.
     * 
     * @param s
     *            The Unicode string.
     * @param collator
     *            The collator.
     *            
     * @return True iff the collector differentiates between the string and its
     *         successor (formed by appending a nul character) in its generated
     *         sort keys.
     */
    protected boolean doSuccessorTest(String s, Properties properties) {

        final DefaultKeyBuilderFactory factory = new DefaultKeyBuilderFactory(properties);
        
        assertEquals(factory.getCollator(), CollatorEnum.ICU);

        assertEquals(factory.getLocale().getLanguage(), Locale.US.getLanguage());

        assertEquals(factory.getLocale().getCountry(), Locale.US.getCountry());
        
        final IKeyBuilder keyBuilder = factory.getKeyBuilder();

        final String successor = SuccessorUtil.successor(s);

        // the successor is one character longer.
        assertEquals(s.length() + 1, successor.length());

        final byte[] key1 = keyBuilder.reset().append(s).getKey();

        final byte[] key2 = keyBuilder.reset().append(successor).getKey();

        // key1 MUST order less than key2.
        final int ret = BytesUtil.compareBytes(key1, key2);

        if (ret >= 0) {
            log
                    .warn("Key1 does NOT order less than successor(key1) : comparator returns "
                            + ret);
            System.err.println("text=" + s);
            System.err.println("strength="
                    + properties.getProperty(Options.STRENGTH));
            System.err.println("key1: " + Arrays.toString(key1));
            System.err.println("key2: " + Arrays.toString(key2));
            return false;
        }

        return true;
        
    }
    
}
