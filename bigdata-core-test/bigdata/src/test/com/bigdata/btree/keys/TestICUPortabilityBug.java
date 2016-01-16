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
 * Created on Mar 17, 2011
 */

package com.bigdata.btree.keys;

import java.util.Arrays;
import java.util.Locale;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.VersionInfo;

/**
 * This is a unit test for a possible ICU portability bug.
 * <p>
 * Note: This issue has been resolved. The problem was that someone had
 * substituted a difference version of ICU on the classpath in the deployed
 * system.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/193
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestICUPortabilityBug.java 4313 2011-03-18 13:16:31Z
 *          thompsonbry $
 */
public class TestICUPortabilityBug extends TestCase {

    private final static Logger log = Logger
            .getLogger(TestICUPortabilityBug.class);  
    
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

        if (log.isInfoEnabled()) {
            log.info("Actual  : " + Arrays.toString(actual));
        }
        
        /*
         * The expected Unicode sort key (this depends on the runtime ICU
         * version).
         */
        final byte[] expected;
        if (VersionInfo.ICU_VERSION.getMajor() == 3
                && VersionInfo.ICU_VERSION.getMinor() == 6) {
            /*
             * bigdata was initially deployed against v3.6.
             */
            expected = new byte[] { 7, -124, 7, -124, 53, 63, 69, 43, 41, 63,
                    75, 69, 85, 77, 79, 69, 75, 49, 1, 20, 1, 126, -113, -124,
                    -113, 8 };
        } else if (VersionInfo.ICU_VERSION.getMajor() == 4
                && VersionInfo.ICU_VERSION.getMinor() == 8) {
            /*
             * The next bundled version was 4.8.
             */
            expected = new byte[] { 6, 12, 6, 12, 51, 61, 67, 41, 39, 61, 73,
                    67, 83, 75, 77, 67, 73, 47, 1, 20, 1, 126, -113, -124,
                    -113, 8};
        } else {

            throw new AssertionFailedError("Not an expected ICU version: "
                    + VersionInfo.ICU_VERSION);

        }

        if (log.isInfoEnabled()) {
            log.info("Expected: " + Arrays.toString(expected));
        }

        if (!Arrays.equals(expected, actual)) {
            fail("Expected: " + Arrays.toString(expected) + ", " + //
                    "Actual: " + Arrays.toString(actual));
        }

    }

}
