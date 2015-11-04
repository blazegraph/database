/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.
Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com
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

import javax.xml.datatype.DatatypeConfigurationException;

import org.apache.log4j.Logger;
import org.junit.Test;

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
public class TestEncodeDecodeLiteralArrayIVs extends TestEncodeDecodeMixedIVs {

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
    public void test01() throws Exception {

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
        }
        
        final AbstractLiteralIV[] ivs = arrays.toArray(new AbstractLiteralIV[arrays.size()]);
        
        if (log.isDebugEnabled()) {
            log.debug(ivs.length);
        }

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

    public void test_encodeDecode_comparator() throws DatatypeConfigurationException {
        // no need to re-test this
    }

}