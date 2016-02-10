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

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.IPv4AddrIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;

/**
 * Encode/decode unit tests for {@link IPv4AddrIV}.
 * 
 * @author <a href="mailto:mike@systap.com">Mike Personick</a>
 * @version $Id$
 */
public class TestEncodeDecodeIPv4AddrIV extends AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public TestEncodeDecodeIPv4AddrIV() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeIPv4AddrIV(String name) {
        super(name);
    }

    /**
     * Unit test for round-trip of IPv4 literals.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test01() throws Exception {

        final AbstractLiteralIV[] ivs = new AbstractLiteralIV[] {
            // legal IPv4 (inline)
            new IPv4AddrIV("1.2.3.4"), 
            new IPv4AddrIV("1.2.3.4/24"),
        };

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

    }

}
