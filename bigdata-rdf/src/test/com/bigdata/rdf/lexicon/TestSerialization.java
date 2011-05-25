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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Locale;
import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.model.TestBigdataValueSerialization;

/**
 * Tests of the {@link LexiconKeyBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @see TestBigdataValueSerialization
 * 
 * @deprecated by the TERMS refactor along with {@link LexiconKeyBuilder}.
 */
public class TestSerialization extends TestCase2 {

    /**
     * 
     */
    public TestSerialization() {
    }

    /**
     * @param arg0
     */
    public TestSerialization(String arg0) {
        super(arg0);
    }

    /**
     * This is an odd issue someone reported for the trunk. There are two
     * version of a plain Literal <code>Brian McCarthy</code>, but it appears
     * that one of the two versions has a leading bell character when you decode
     * the Unicode byte[]. I think that this is actually an issue with the
     * {@link Locale} and the Unicode sort key generation. If {@link KeyBuilder}
     * as configured on the system generates Unicode sort keys which compare as
     * EQUAL for these two inputs then that will cause the lexicon to report an
     * "apparent" inconsistency. In fact, what we probably need to do is just
     * disable the inconsistency check in the lexicon.
     * 
     * <pre>
     * ERROR: com.bigdata.rdf.lexicon.Id2TermWriteProc.apply(Id2TermWriteProc.java:205): val=[0, 2, 0, 14, 66, 114, 105, 97, 110, 32, 77, 99, 67, 97, 114, 116, 104, 121]
     * ERROR: com.bigdata.rdf.lexicon.Id2TermWriteProc.apply(Id2TermWriteProc.java:206): oldval=[0, 2, 0, 15, 127, 66, 114, 105, 97, 110, 32, 77, 99, 67, 97, 114, 116, 104, 121]
     * </pre>
     */
    public void test_consistencyIssue() {

        final BigdataValueSerializer<Value> fixture = new BigdataValueSerializer<Value>(
                ValueFactoryImpl.getInstance());

        final byte[] newValBytes = new byte[] { 0, 2, 0, 14, 66, 114, 105, 97, 110, 32,
                77, 99, 67, 97, 114, 116, 104, 121 };

        final byte[] oldValBytes = new byte[] { 0, 2, 0, 15, 127, 66, 114, 105,
                97, 110, 32, 77, 99, 67, 97, 114, 116, 104, 121 };

        final Value newValue = fixture.deserialize(newValBytes);

        final Value oldValue = fixture.deserialize(oldValBytes);

        if (log.isInfoEnabled()) {
            
            log.info("new=" + newValue);

            log.info("old=" + oldValue);
            
        }

        /*
         * Note: This uses the default Locale and the implied Unicode collation
         * order to generate the sort keys.
         */
//        final IKeyBuilder keyBuilder = new KeyBuilder();

        /*
         * Note: This allows you to explicitly configure the behavior of the
         * KeyBuilder instance based on the specified properties.  If you want
         * your KB to run with these properties, then you need to specify them
         * either in your environment or using -D to java.
         */
        final Properties properties = new Properties();
        
        // specify that all aspects of the Unicode sequence are significant.
        properties.setProperty(KeyBuilder.Options.STRENGTH,StrengthEnum.Identical.toString());
        
//        // specify that that only primary character differences are significant.
//        properties.setProperty(KeyBuilder.Options.STRENGTH,StrengthEnum.Primary.toString());
        
        final IKeyBuilder keyBuilder = KeyBuilder
                .newUnicodeInstance(properties);

        final LexiconKeyBuilder lexKeyBuilder = new LexiconKeyBuilder(
                keyBuilder);

        // encode as unsigned byte[] key.
        final byte[] newValKey = lexKeyBuilder.value2Key(newValue);

        final byte[] oldValKey = lexKeyBuilder.value2Key(oldValue);

        if (log.isInfoEnabled()) {

            log.info("newValKey=" + BytesUtil.toString(newValKey));

            log.info("oldValKey=" + BytesUtil.toString(oldValKey));

        }

        /*
         * Note: if this assert fails then the two distinct Literals were mapped
         * onto the same unsigned byte[] key.
         */
        assertFalse(BytesUtil.bytesEqual(newValKey, oldValKey));

    }

}
