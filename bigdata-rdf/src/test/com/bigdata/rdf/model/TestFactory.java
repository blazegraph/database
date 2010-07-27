/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jul 26, 2010
 */

package com.bigdata.rdf.model;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.omg.CORBA.portable.ValueFactory;

import junit.framework.TestCase2;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFactory extends TestCase2 {

    /**
     * 
     */
    public TestFactory() {
    }

    /**
     * @param name
     */
    public TestFactory(String name) {
        super(name);
    }

    public void test_gregorian() throws DatatypeConfigurationException {

        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance(getName());

        final XMLGregorianCalendar cal = DatatypeFactory.newInstance().newXMLGregorianCalendarDate(
                2010,// year
                1, // month,
                13, // day,
                0// timezone
                );
        
        assertEquals("http://www.w3.org/2001/XMLSchema#date", vf
                .createLiteral(cal).getDatatype().stringValue());

    }

}
