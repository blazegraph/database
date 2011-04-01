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

package com.bigdata.rdf.internal;

import java.util.GregorianCalendar;
import java.util.TimeZone;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * This implementation of {@link IExtension} implements inlining for literals
 * that represent xsd:dateTime literals.  These literals will be stored as time 
 * in milliseconds since the epoch.  The milliseconds are encoded as an inline 
 * long.
 */
public class DateTimeExtension<V extends BigdataValue> implements IExtension<V> {

    private final BigdataURI dateTime;
    
    private final TimeZone defaultTZ;
    
    public DateTimeExtension(final IDatatypeURIResolver resolver, 
    		final TimeZone defaultTZ) {

        this.dateTime = resolver.resolve(XSD.DATETIME);
        this.defaultTZ = defaultTZ;
        
    }
        
    public BigdataURI getDatatype() {
        
        return dateTime;
        
    }
    
    /**
     * Attempts to convert the supplied value into an epoch representation.
     * Tests for a literal value with the correct datatype that can be converted 
     * to a positive long integer.  Encodes the long in a delegate 
     * {@link XSDLongIV}, and returns an {@link ExtensionIV} to wrap the native
     * type.
     */
    public ExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        final Literal lit = (Literal) value;
        
        final URI dt = lit.getDatatype();
        
        if (dt == null || !XSD.DATETIME.stringValue().equals(dt.stringValue()))
            throw new IllegalArgumentException();
        
        final String s = value.stringValue();
        
        final XMLGregorianCalendar c = XMLDatatypeUtil.parseCalendar(s);
        
        if (c.getTimezone() == DatatypeConstants.FIELD_UNDEFINED) {
        	final int offsetInMillis = 
//        		defaultTZ.getRawOffset();
        		defaultTZ.getOffset(c.toGregorianCalendar().getTimeInMillis());
        	final int offsetInMinutes = 
        		offsetInMillis / 1000 / 60;
        	c.setTimezone(offsetInMinutes);
        }

        /*
         * Returns the current time as UTC milliseconds from the epoch
         */
        final long l = c.toGregorianCalendar().getTimeInMillis();

        final AbstractLiteralIV delegate = new XSDLongIV(l);

        return new ExtensionIV(delegate, (TermId) getDatatype().getIV());
        
    }
    
    /**
     * Use the long value of the {@link XSDLongIV} delegate (which represents
     * milliseconds since the epoch) to create a an XMLGregorianCalendar
     * object (GMT timezone).  Use the XMLGregorianCalendar to create a datatype
     * literal value with the xsd:dateTime datatype.
     */
    public V asValue(final ExtensionIV iv, final BigdataValueFactory vf) {
        
    	/*
    	 * Milliseconds since the epoch.
    	 */
    	final long l = iv.getDelegate().longValue();
    	
		final TimeZone tz = BSBMHACK ? TimeZone.getDefault()/*getTimeZone("GMT")*/ : defaultTZ;
    	final GregorianCalendar c = new GregorianCalendar(tz);
		c.setTimeInMillis(l);
    	
    	try {
    		
	    	final XMLGregorianCalendar xmlGC = 
	    		DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

			String s = xmlGC.toString();
			if (BSBMHACK) {
				// Chopping off the milliseconds part and the trailing 'Z'.
				final int i = s.lastIndexOf('.');
				if (i >= 0) {
					s = s.substring(0, i);
				}
			}
	    	
	        return (V) vf.createLiteral(s, dateTime);

    	} catch (DatatypeConfigurationException ex) {
    		
    		throw new IllegalArgumentException("bad iv: " + iv, ex);
    		
    	}
    	
    }

	/**
	 * This conditionally enables some logic for xsd:dateTime compatibility with
	 * BSBM.
	 * 
	 * @see http://sourceforge.net/apps/trac/bigdata/ticket/277
	 */
    static private transient boolean BSBMHACK = Boolean.getBoolean("BSBM_HACK");
    
}
