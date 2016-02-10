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

import java.util.UUID;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Unit tests for {@link PackedLongIV} and its associated 
 * {@link CompressedTimestampExtension} extension.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestEncodeDecodePackedLongIVs extends
      AbstractEncodeDecodeKeysTestCase {

   /**
     * 
     */
   public TestEncodeDecodePackedLongIVs() {
   }

   /**
    * @param name
    */
   public TestEncodeDecodePackedLongIVs(String name) {
      super(name);
   }

    @SuppressWarnings("rawtypes")
    public void testRoundTripAndComparePackedLongIV() throws Exception {

        final AbstractLiteralIV[] ivs = new AbstractLiteralIV[] {
                new PackedLongIV("0"), 
                new PackedLongIV("100"),
                new PackedLongIV("101"),
                new PackedLongIV("103"),
                new PackedLongIV("1000"),
                new PackedLongIV("1010"),
                new PackedLongIV("1011"), 
                new PackedLongIV("1012"),
                new PackedLongIV("1013"), 
                new PackedLongIV("1014"),
                new PackedLongIV("1015"), 
                new PackedLongIV("1016"),
                new PackedLongIV("1017"),
                new PackedLongIV("1018"),
                new PackedLongIV("1019"),
                new PackedLongIV("1020"),
                new PackedLongIV("1021"),
                new PackedLongIV("1022"),
                new PackedLongIV("1023"),
                new PackedLongIV("1024"),
                new PackedLongIV("1025"),
                new PackedLongIV("1026"),
                new PackedLongIV("10000"),
                new PackedLongIV("10010"),
                new PackedLongIV("10100"),
                new PackedLongIV("11000"),
                new PackedLongIV("10000"),
                new PackedLongIV("100001"),
                new PackedLongIV("1000000"),
                new PackedLongIV("10000000"),

                // from here: some realistic timestamp (up to year 9999)
                new PackedLongIV("1446203550"), 
                new PackedLongIV("1446203560"),
                new PackedLongIV("1446203570"), 
                new PackedLongIV("1446203580"),
                new PackedLongIV("1446203590"), 
                new PackedLongIV("1446203600"),
                new PackedLongIV("1448881949"), 
                new PackedLongIV("1480504349"),
                new PackedLongIV("2553419549"), 
                new PackedLongIV("2869038749"),
                new PackedLongIV("3500190749"), 
                new PackedLongIV("4131256349"),
                new PackedLongIV("7286929949"),
                new PackedLongIV("16754037149"),
                new PackedLongIV("32532491549"),
                new PackedLongIV("127203390749"),
                new PackedLongIV("253399576349") };

        doEncodeDecodeTest(ivs);
        doComparatorTest(ivs);

    }
    
    @SuppressWarnings("rawtypes")
    public void testRoundTripAndCompareCompressedTimestamp() throws Exception {
        
        // namespaces should never be reused in test suites.
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance(getName() + UUID.randomUUID());

        final CompressedTimestampExtension<BigdataValue> ext = 
            new CompressedTimestampExtension<BigdataValue>(
                new IDatatypeURIResolver() {
                      @Override
                      public BigdataURI resolve(final URI uri) {
                         final BigdataURI buri = vf.createURI(uri.stringValue());
                         buri.setIV(newTermId(VTE.URI));
                         return buri;
                      }
                });

        // we'll create a permutation over all values above
        final BigdataLiteral[] dt = {
             vf.createLiteral("0", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("100", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("101", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("103", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1000", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1010", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1011", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1012", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1013", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1014", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1015", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1016", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1017", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1018", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1019", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1020", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1021", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1022", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1023", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1024", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1025", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1026", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("10000", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("10010", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("10100", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("11000", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("100000", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("100001", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1000000", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("10000000", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             
             // from here: some realistic timestamp (up to year 9999)
             vf.createLiteral("1446203550", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1446203560", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1446203570", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1446203580", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1446203590", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1446203600", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1448881949", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("1480504349", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("2553419549", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("2869038749", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("3500190749", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("4131256349", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("7286929949", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("16754037149", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("32532491549", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("127203390749", CompressedTimestampExtension.COMPRESSED_TIMESTAMP),
             vf.createLiteral("253399576349", CompressedTimestampExtension.COMPRESSED_TIMESTAMP)
        };

        final IV<?, ?>[] e = new IV[dt.length];
        for (int i = 0; i < dt.length; i++) {
            e[i] = ext.createIV(dt[i]);
         }
        
        for (int i = 0; i < e.length; i++) {
            final BigdataValue val = ext.asValue((LiteralExtensionIV) e[i], vf);
            
            // verify val has been correctly round-tripped
              if (log.isInfoEnabled())
                  log.info(val);
          }
        
         doEncodeDecodeTest(e);
         doComparatorTest(e);
         
    }
    

    @SuppressWarnings("rawtypes")
    public void testPackedLongIVOutOfRange() {
       
        // first failing lower
        boolean failsLower = false;
        try {
            new PackedLongIV("-1"); 
        } catch (Exception e) {
            failsLower = true; // expected
        }
        assertTrue(failsLower);
        
        // first succeeding lower
        new PackedLongIV("0"); 
        
        // last succeeding upper
        new PackedLongIV("72057594037927935"); 
        
        // first failing upper
        boolean failsUpper = false;
        try {
            new PackedLongIV("72057594037927936"); 
        } catch (Exception e) {
            failsUpper = true; // expected
        }
        assertTrue(failsUpper);

    }
   
   
}
