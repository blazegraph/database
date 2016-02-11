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
 * Created on Feb 10, 2016
 */

package com.bigdata.rdf.internal;

import java.util.TimeZone;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Unit tests for {@link XSDIntegerIV}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestEncodeDecodeXSDDateIVs extends
        AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public TestEncodeDecodeXSDDateIVs() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeXSDDateIVs(String name) {
        super(name);
    }

    /**
     * Unit test for xsd:date literal encoding.
     */
    public void test_encodeDecodeDateLiterals() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01", XSD.DATE),
               vf.createLiteral("-2015-12-31", XSD.DATE),
               vf.createLiteral("9999-01-01", XSD.DATE),
               vf.createLiteral("9999-12-31", XSD.DATE)
       };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           
           assertEquals(valRoundTrip, dt[i] /* original value */);
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }

    /**
     * Unit test for xsd:gDay literal encoding.
     */
    public void test_encodeDecodeGDay() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01", XSD.GDAY),
               vf.createLiteral("-2015-12-31", XSD.GDAY),
               vf.createLiteral("9999-01-01", XSD.GDAY),
               vf.createLiteral("9999-12-31", XSD.GDAY)
       };
       
       final String[] expected = { "---01", "---31", "---01", "---31" };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + "\"^^<http://www.w3.org/2001/XMLSchema#gDay>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    /**
     * Unit test for xsd:gMonth literal encoding.
     */
    public void test_encodeDecodeGMonth() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01", XSD.GMONTH),
               vf.createLiteral("-2015-12-31", XSD.GMONTH),
               vf.createLiteral("9999-01-01", XSD.GMONTH),
               vf.createLiteral("9999-12-31", XSD.GMONTH)
       };
       
       final String[] expected = { "--01", "--12", "--01", "--12" };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + "\"^^<http://www.w3.org/2001/XMLSchema#gMonth>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    /**
     * Unit test for xsd:gMonthDay literal encoding.
     */
    public void test_encodeDecodeGMonthDay() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01", XSD.GMONTHDAY),
               vf.createLiteral("-2015-12-31", XSD.GMONTHDAY),
               vf.createLiteral("9999-01-01", XSD.GMONTHDAY),
               vf.createLiteral("9999-12-31", XSD.GMONTHDAY)
       };
       
       final String[] expected = { "--01-01", "--12-31", "--01-01", "--12-31" };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + "\"^^<http://www.w3.org/2001/XMLSchema#gMonthDay>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    /**
     * Unit test for xsd:gYear literal encoding.
     */
    public void test_encodeDecodeGYear() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01", XSD.GYEAR),
               vf.createLiteral("-1000-12-31", XSD.GYEAR),
               vf.createLiteral("0001-01-01", XSD.GYEAR),
               vf.createLiteral("9999-12-31", XSD.GYEAR)
       };
       
       final String[] expected = { "-2015", "-1000", "0001", "9999" };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + "\"^^<http://www.w3.org/2001/XMLSchema#gYear>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    /**
     * Unit test for xsd:gYear literal encoding.
     */
    public void test_encodeDecodeGYearMonth() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01", XSD.GYEARMONTH),
               vf.createLiteral("-1000-12-31", XSD.GYEARMONTH),
               vf.createLiteral("0001-01-01", XSD.GYEARMONTH),
               vf.createLiteral("9999-12-31", XSD.GYEARMONTH)
       };
       
       final String[] expected = { "-2015-01", "-1000-12", "0001-01", "9999-12" };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + "\"^^<http://www.w3.org/2001/XMLSchema#gYearMonth>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    
    /**
     * Unit test for xsd:dateTime literal encoding.
     */
    public void test_encodeDecodeDateTime() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("-2015-01-01T10:10:10", XSD.DATETIME),
               vf.createLiteral("-1000-12-31T00:00:00", XSD.DATETIME),
               vf.createLiteral("0001-01-01T23:59:59", XSD.DATETIME),
               vf.createLiteral("9999-12-31T12:12:12", XSD.DATETIME)
       };
       
       final String[] expected = { 
           "-2015-01-01T10:10:10", 
           "-1000-12-31T00:00:00", 
           "0001-01-01T23:59:59", 
           "9999-12-31T12:12:12" };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + ".000Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    /**
     * Unit test for xsd:dateTime literal encoding.
     */
    public void test_encodeDecodeTime() throws Exception {

       final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
       
       final DateTimeExtension<BigdataValue> ext =  getDateTimeExtensionGMT(vf);
       
       final BigdataLiteral[] dt = {
               vf.createLiteral("00:00:00", XSD.TIME),
               vf.createLiteral("01:02:03", XSD.TIME),
               vf.createLiteral("10:20:30", XSD.TIME),
               vf.createLiteral("23:59:59", XSD.TIME)
       };
       
       // create associated IVs
       final IV<?, ?>[] e = new IV[dt.length];
       for (int i = 0; i < dt.length; i++) {
          e[i] = ext.createIV(dt[i]);
       }
       
       final String[] expected = { "00:00:00", "01:02:03", "10:20:30", "23:59:59" };
       
       for (int i = 0; i < e.length; i++) {
           @SuppressWarnings("rawtypes")
           final BigdataValue valRoundTrip = ext.asValue((LiteralExtensionIV) e[i], vf);
           
           assertEquals(valRoundTrip.toString(), "\"" + expected[i] + ".000Z\"^^<http://www.w3.org/2001/XMLSchema#time>" );
        }
       
       final IV<?, ?>[] a = doEncodeDecodeTest(e);
       
       doComparatorTest(e);
    }
    
    
    /**
     * Get a {@link DateTimeExtension} object.
     */
    protected DateTimeExtension<BigdataValue> getDateTimeExtensionGMT(final BigdataValueFactory vf) {
       
       return 
          new DateTimeExtension<BigdataValue>(
             new IDatatypeURIResolver() {
                public BigdataURI resolve(URI uri) {
                   final BigdataURI buri = vf.createURI(uri.stringValue());
                   buri.setIV(newTermId(VTE.URI));
                   return buri;
                }
          },TimeZone.getTimeZone("GMT"));
    }
    
}
