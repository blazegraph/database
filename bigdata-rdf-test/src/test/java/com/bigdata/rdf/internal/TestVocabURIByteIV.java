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
 * Created on Dec 21, 2011
 */

package com.bigdata.rdf.internal;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.util.BytesUtil;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVocabURIByteIV extends AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public TestVocabURIByteIV() {
    }

    /**
     * @param name
     */
    public TestVocabURIByteIV(String name) {
        super(name);
    }

    /**
     * Unit test for a fully inlined representation of a URI based on a
     * <code>byte</code> code. The flags byte looks like:
     * <code>VTE=URI, inline=true, extension=false,
     * DTE=XSDByte</code>. It is followed by a <code>unsigned byte</code> value
     * which is the index of the URI in the {@link Vocabulary} class for the
     * triple store.
     */
    public void test_encodeDecode_URIByteIV() {

        final IV<?, ?>[] e = {//
//                new VocabURIByteIV<BigdataURI>((byte) Byte.MIN_VALUE),//
//                new VocabURIByteIV<BigdataURI>((byte) -1),//
                new VocabURIByteIV<BigdataURI>((byte) 0),//
                new VocabURIByteIV<BigdataURI>((byte) KeyBuilder.encodeByte(14)),//
                new VocabURIByteIV<BigdataURI>((byte) 14),//
//                new VocabURIByteIV<BigdataURI>((byte) Byte.MAX_VALUE),//
        };

        final KeyBuilder keyBuilder = new KeyBuilder();
        for (IV iv : e) {
            System.out.println("iv="
                    + iv
                    + ", key="
                    + BytesUtil.toString(IVUtility.encode(keyBuilder.reset(),
                            iv).getKey()));
        }
        
        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

}
