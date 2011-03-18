/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 17, 2011
 */

package com.bigdata.btree.keys;

import java.util.Arrays;
import java.util.Locale;

import junit.framework.TestCase;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * This is a unit test for a possible ICU portability bug.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/193
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestICUPortabilityBug extends TestCase {

    /**
     * 
     */
    public TestICUPortabilityBug() {
    }

    /**
     * @param name
     */
    public TestICUPortabilityBug(String name) {
        super(name);
    }

    /**
     * Unit test for ICU generation of Unicode sort keys.
     * <pre>
     * Input   : "__globalRowStore"
     * 
     * Expected: [7, -124, 7, -124, 53, 63, 69, 43, 41, 63, 75, 69, 85, 77, 79, 69, 75, 49, 1, 20, 1, 126, -113, -124, -113, 8]
     * </pre>
     */
    public void test_ICU_Unicode_SortKey() {
        
        final String input = "__globalRowStore";

        // The expected Unicode sort key.
        final byte[] expected = new byte[] { 7, -124, 7, -124, 53, 63, 69, 43,
                41, 63, 75, 69, 85, 77, 79, 69, 75, 49, 1, 20, 1, 126, -113,
                -124, -113, 8 };

        // Buffer reused for each String from which a sort key is derived.
        final RawCollationKey raw = new RawCollationKey(128);

        /*
         * Setup the collator by specifying the locale, strength, and
         * decomposition mode.
         */
        final Locale locale = new Locale("en", "US");
        
        final RuleBasedCollator collator = (RuleBasedCollator) Collator
                .getInstance(locale);

        collator.setStrength(Collator.TERTIARY);

        collator.setDecomposition(Collator.NO_DECOMPOSITION);

        collator.getRawCollationKey(input, raw);

        // do not include the nul byte
        final byte[] actual = new byte[raw.size - 1];

        // copy data from the buffer.
        System.arraycopy(raw.bytes/* src */, 0/* srcPos */, actual/* dest */,
                0/* destPos */, actual.length);

        System.err.println("Expected: " + Arrays.toString(expected));
        System.err.println("Actual  : " + Arrays.toString(actual));

        if (!Arrays.equals(expected, actual)) {
            fail("Expected: " + Arrays.toString(expected) + ", " + //
                 "Actual: " + Arrays.toString(actual));
        }

    }
    
}
