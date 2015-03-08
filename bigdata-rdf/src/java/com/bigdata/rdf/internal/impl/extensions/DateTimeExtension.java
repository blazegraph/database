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

package com.bigdata.rdf.internal.impl.extensions;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
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

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.util.InnerCause;

/**
 * This implementation of {@link IExtension} implements inlining for literals
 * that represent xsd:dateTime literals.  These literals will be stored as time 
 * in milliseconds since the epoch.  The milliseconds are encoded as an inline 
 * long.
 */
public class DateTimeExtension<V extends BigdataValue> implements IExtension<V> {

	private static final transient Logger log = Logger.getLogger(DateTimeExtension.class);
	
	
    private final Map<IV,BigdataURI> datatypes;
    
    private final TimeZone defaultTZ;
    
    public DateTimeExtension(final IDatatypeURIResolver resolver, 
            final TimeZone defaultTZ) {

//        this.dateTime = resolver.resolve(XSD.DATETIME);
        this.datatypes = new LinkedHashMap<IV,BigdataURI>();
        resolve(resolver, XSD.DATETIME);
        resolve(resolver, XSD.DATE);
        resolve(resolver, XSD.TIME);
        resolve(resolver, XSD.GDAY);
        resolve(resolver, XSD.GMONTH);
        resolve(resolver, XSD.GMONTHDAY);
        resolve(resolver, XSD.GYEAR);
        resolve(resolver, XSD.GYEARMONTH);
        
        this.defaultTZ = defaultTZ;
        
    }
    
    private void resolve(final IDatatypeURIResolver resolver, final URI uri) {

        if (log.isDebugEnabled()) {
            log.debug("resolving: " + uri);
        }
    	
        final BigdataURI val = resolver.resolve(uri);
        datatypes.put(val.getIV(), val);
        
    }

    @Override
    public Set<BigdataURI> getDatatypes() {
        
        return new LinkedHashSet<BigdataURI>(datatypes.values());
        
    }
    
    /**
     * Attempts to convert the supplied value into an epoch representation.
     * Tests for a literal value with the correct datatype that can be converted 
     * to a positive long integer.  Encodes the long in a delegate 
     * {@link XSDLongIV}, and returns an {@link LiteralExtensionIV} to wrap the native
     * type.
     */
    public LiteralExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        final Literal lit = (Literal) value;
        
        final URI dt = lit.getDatatype();
        
        if (dt == null)
            throw new IllegalArgumentException();
        
        BigdataURI resolvedDT = null;
        for (BigdataURI val : datatypes.values()) {
            // Note: URI.stringValue() is efficient....
            if (val.stringValue().equals(dt.stringValue())) {
                resolvedDT = val;
            }
        }
        
        if (resolvedDT == null)
            throw new IllegalArgumentException();
        
        final String s = value.stringValue();
        
        final XMLGregorianCalendar c = XMLDatatypeUtil.parseCalendar(s);
        
        if (c.getTimezone() == DatatypeConstants.FIELD_UNDEFINED) {
            final GregorianCalendar gc = c.toGregorianCalendar();
            gc.setGregorianChange(new Date(Long.MIN_VALUE));
            
            final int offsetInMillis = 
//                defaultTZ.getRawOffset();
                defaultTZ.getOffset(gc.getTimeInMillis());
            final int offsetInMinutes = 
                offsetInMillis / 1000 / 60;
            c.setTimezone(offsetInMinutes);
        }

        final GregorianCalendar gc = c.toGregorianCalendar();
        gc.setGregorianChange(new Date(Long.MIN_VALUE));
        
        /*
         * Returns the current time as UTC milliseconds from the epoch
         */
        final long l = gc.getTimeInMillis();

        final AbstractLiteralIV delegate = new XSDNumericIV(l);

        return new LiteralExtensionIV(delegate, resolvedDT.getIV());
        
    }
    
    /**
     * Use the long value of the {@link XSDLongIV} delegate (which represents
     * milliseconds since the epoch) to create a an XMLGregorianCalendar
     * object (GMT timezone).  Use the XMLGregorianCalendar to create a datatype
     * literal value with the appropriate datatype.
     */
    public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {
        
        if (!datatypes.containsKey(iv.getExtensionIV())) {
            throw new IllegalArgumentException("unrecognized datatype");
        }
        
        /*
         * Milliseconds since the epoch.
         */
        final long l = iv.getDelegate().longValue();
        
        final TimeZone tz = BSBMHACK ? TimeZone.getDefault()/*getTimeZone("GMT")*/ : defaultTZ;
        final GregorianCalendar c = new GregorianCalendar(tz);
        c.setGregorianChange(new Date(Long.MIN_VALUE));
        c.setTimeInMillis(l);
        
        try {
            
            final BigdataURI dt = datatypes.get(iv.getExtensionIV());
            
            final DatatypeFactory f = datatypeFactorySingleton;

            final XMLGregorianCalendar xmlGC = f.newXMLGregorianCalendar(c);

            String s = xmlGC.toString();
            if (dt.equals(XSD.DATETIME)) {
                if (BSBMHACK) {
                    // Chopping off the milliseconds part and the trailing 'Z'.
                    final int i = s.lastIndexOf('.');
                    if (i >= 0) {
                        s = s.substring(0, i);
                    }
                }
            } else if (dt.equals(XSD.DATE)) {
                // YYYY-MM-DD (10 chars)
                s = s.substring(0, 10);
            } else if (dt.equals(XSD.TIME)) {
                // everything after the date (from 11 chars in)
                s = s.substring(10);
            } else if (dt.equals(XSD.GDAY)) {
                // gDay Defines a part of a date - the day (---DD)
                s = "---" + s.substring(8, 10);
            } else if (dt.equals(XSD.GMONTH)) {
                // gMonth Defines a part of a date - the month (--MM)
                s = "--" + s.substring(5, 7);
            } else if (dt.equals(XSD.GMONTHDAY)) {
                // gMonthDay Defines a part of a date - the month and day (--MM-DD)
                s = "--" + s.substring(5, 10);
            } else if (dt.equals(XSD.GYEAR)) {
                // gYear Defines a part of a date - the year (YYYY)
                s = s.substring(0, 4);
            } else if (dt.equals(XSD.GYEARMONTH)) {
                // gYearMonth    Defines a part of a date - the year and month (YYYY-MM)
                s = s.substring(0, 7);
            } 
            
            return (V) vf.createLiteral(s, dt);

        } catch (RuntimeException ex) {

            if (InnerCause.isInnerCause(ex, InterruptedException.class)) {

                throw ex;

            }
            
            throw new IllegalArgumentException("bad iv: " + iv, ex);
            
        }

    }

    /** Singleton. */
    private static final DatatypeFactory datatypeFactorySingleton;

    /**
     * Singleton caching pattern for the Datatype factory reference.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/802">
     *      Optimize DatatypeFactory instantiation in DateTimeExtension </a>
     */
    static {

        DatatypeFactory f = null;
        
        try {

            f = DatatypeFactory.newInstance();

        } catch (DatatypeConfigurationException ex) {

            log.error("Could not configure DatatypeFactory: " + ex, ex);

        }

        datatypeFactorySingleton = f;

    }

    /**
     * This conditionally enables some logic for xsd:dateTime compatibility with
     * BSBM.
     * 
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/277
     */
    static private transient boolean BSBMHACK = Boolean.getBoolean("BSBM_HACK");

}
