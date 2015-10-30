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

import java.util.UUID;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Unit tests for {@link CompressedTimestampExtension}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestEncodeDecodeCompressedTimestamp extends
      AbstractEncodeDecodeKeysTestCase {

   /**
     * 
     */
   public TestEncodeDecodeCompressedTimestamp() {
   }

   /**
    * @param name
    */
   public TestEncodeDecodeCompressedTimestamp(String name) {
      super(name);
   }

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
          @SuppressWarnings("rawtypes")
          final BigdataValue val = ext.asValue((LiteralExtensionIV) e[i], vf);
          
          // verify val has been correctly round-tripped
            if (log.isInfoEnabled())
                log.info(val);
        }
      
       doEncodeDecodeTest(e);
       doComparatorTest(e);
       
   }
   
   
}