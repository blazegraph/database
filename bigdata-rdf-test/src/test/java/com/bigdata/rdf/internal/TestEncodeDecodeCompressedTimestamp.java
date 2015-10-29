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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription.Datatype;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.service.geospatial.GeoSpatial;

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

   /**
    * Unit test for round-trip of GeoSpatial literals of lat+lon+time
    * GeoSpatial literals.
    */
   public void test01() throws Exception {

      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
      
      final CompressedTimestampExtension<BigdataValue> ext = 
          new CompressedTimestampExtension<BigdataValue>(
              new IDatatypeURIResolver() {
                    public BigdataURI resolve(URI uri) {
                       final BigdataURI buri = vf.createURI(uri.stringValue());
                       buri.setIV(newTermId(VTE.URI));
                       return buri;
                    }
              });

      // we'll create a permutation over all values above
      final BigdataLiteral[] dt = new BigdataLiteral[1];
      dt[0] = vf.createLiteral("12345678", CompressedTimestampExtension.COMPRESSED_TIMESTAMP);
      // TODO: may add some more here

      final IV<?, ?>[] e = new IV[dt.length];
      for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
      
      for (int i = 0; i < e.length; i++) {
          @SuppressWarnings("rawtypes")
          final BigdataValue val = ext.asValue((LiteralExtensionIV) e[i], vf);
          
          // verify val has been correctly round-tripped
          System.out.println(val);
       }
   }
}
