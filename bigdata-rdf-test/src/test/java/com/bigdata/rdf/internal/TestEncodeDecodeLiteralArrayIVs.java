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
 * Created on August 31, 2015
 */

package com.bigdata.rdf.internal;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralArrayIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;

/**
 * Encode/decode unit tests for {@link LiteralArrayIV}.
 * 
 * @author <a href="mailto:mike@systap.com">Mike Personick</a>
 * @version $Id$
 */
public class TestEncodeDecodeLiteralArrayIVs extends AbstractEncodeDecodeMixedIVsTest {

    private static final transient Logger log = Logger.getLogger(TestEncodeDecodeLiteralArrayIVs.class);
    
    /**
     * 
     */
    public TestEncodeDecodeLiteralArrayIVs() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeLiteralArrayIVs(String name) {
        super(name);
    }

    /**
     * Encode/decode unit test for {@link LiteralArrayIV}.
     */
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRoundTripLiteralArrayIV() throws Exception {

        final List<IV<?,?>> mixed = super.prepareIVs();
        
        final List<LiteralArrayIV> arrays = new LinkedList<>();
        {
            final int n = 10;
            InlineLiteralIV[] inlines = new InlineLiteralIV[n];
            int i = 0;
            for (IV iv : mixed) {
                
                if (!(iv instanceof InlineLiteralIV)) {
                    continue;
                }
                
                inlines[i++] = (InlineLiteralIV) iv;
                
                // reset
                if (i == n) {
                    arrays.add(new LiteralArrayIV(inlines));
                    inlines = new InlineLiteralIV[n];
                    i = 0;
                }
                
            }
            
            if (i > 0) {
                final InlineLiteralIV[] tmp = new InlineLiteralIV[i];
                System.arraycopy(inlines, 0, tmp, 0, i);
                arrays.add(new LiteralArrayIV(tmp));
            }
        }
        
        final AbstractLiteralIV[] ivs = arrays.toArray(new AbstractLiteralIV[arrays.size()]);
        
        byte vocab = 1;

        final LiteralExtensionIV[] lits = new LiteralExtensionIV[ivs.length];
        for (int i = 0; i < ivs.length; i++) {
            lits[i] = new LiteralExtensionIV(ivs[i], new VocabURIByteIV(vocab++));
        }

        final URIExtensionIV[] uris = new URIExtensionIV[ivs.length];
        for (int i = 0; i < ivs.length; i++) {
            uris[i] = new URIExtensionIV(ivs[i], new VocabURIByteIV(vocab++));
        }
        
        doEncodeDecodeTest(ivs);
        doEncodeDecodeTest(lits);
        doEncodeDecodeTest(uris);
        
        final LiteralArrayIV arrayOfArrays = new LiteralArrayIV(ivs);
        doEncodeDecodeTest(new IV[] { arrayOfArrays });

    }

    public void testRoundTripAsByte() {
        
        testRoundTripAsByte(1);
        testRoundTripAsByte(10);
        testRoundTripAsByte(200);
        testRoundTripAsByte(256);
        
        testRoundTripAsByte(-1);
        testRoundTripAsByte(0);
        testRoundTripAsByte(257);
        
    }
    
    private void testRoundTripAsByte(final int i) {
        // no need to re-test this

        final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
        
//        System.out.println(i);
    
        // int(1...256) --> byte(0...255)
        final byte len = (byte) (i-1);
        keyBuilder.append(len);
//        System.out.println(len);
        
        final byte[] key = keyBuilder.getKey();
        
        assertEquals(1, key.length);
        
        // byte(0...255) --> int(1...256) 
        final int n = ((int) key[0] & 0xFF) + 1;
//        System.out.println(n);
        
        if (i >= 1 && i <= 256) {
            assertEquals(i, n);
        } else {
            assertFalse(i == n);
        }
        
    }

}
