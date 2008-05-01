/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Iterator;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

/**
 * A collection of named {@link Counter}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICounterSet extends ICounterNode {

    /**
     * Separator for path name components.
     */
    public static final String pathSeparator = "/";
    
    /**
     * Visits {@link ICounter} matching the optional filter declared anywhere in
     * the hierarchy spanned by this {@link ICounterSet}.
     * 
     * @param filter
     *            An optional regular expression that will be applied to
     *            {@link ICounter#getPath()} to filter matches.  When specified,
     *            only {@link ICounter}s whose {@link ICounter#getPath()} match
     *            will be visited by the {@link Iterator}.
     */
    public Iterator<ICounter> getCounters(Pattern filter);
    
    /**
     * Adds any necessary {@link ICounterSet}s described in the path (ala
     * mkdirs).
     * 
     * @param path
     *            The path (may be relative or absolute).
     * 
     * @return The {@link ICounterSet} described by the path.
     * 
     * @throws IllegalArgumentException
     *             if the path is <code>null</code>
     * @throws IllegalArgumentException
     *             if the path is an empty string.
     */
    public ICounterSet makePath(String path);
    
    /**
     * A human readable representation of all counters in the hierarchy together
     * with their current value.
     */
    public String toString();

    /**
     * A human readable representation of the counters in the hierarchy together
     * with their current value.
     * 
     * @param filter
     *            An optional filter that will be used to select only specific
     *            counters.
     */
    public String toString(Pattern filter);

    /**
     * Write an XML reprentation of the counters in the hierarchy together with
     * their current value.
     * 
     * @param os
     *            The sink on which the representation will be written.
     * @param encoding
     *            The character set encoding that will be used, e.g.,
     *            <code>UTF-8</code>.
     * @param filter
     *            An optional filter that will be used to select only specific
     *            counters.
     */
    public void asXML(OutputStream os, String encoding, Pattern filter) throws IOException;

    /**
     * Write an XML reprentation of the counters in the hierarchy together with
     * their current value - does not write the XML declaration element since
     * the encoding is unknown.
     * 
     * @param w
     *            The sink on which the representation will be written.
     * @param filter
     *            An optional filter that will be used to select only specific
     *            counters.
     */
    public void asXML(Writer w, Pattern filter) throws IOException;

    /**
     * Writes out the {@link ICounterSet} as XML on a string and returns that
     * string.
     * 
     * @param filter
     *            An optional filter.
     * 
     * @throws IOException
     */
    public String asXML(Pattern filter);
    
    /**
     * A factory for {@link IInstrument}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IInstrumentFactory {
        
        public IInstrument newInstance(Class type);
        
    }
    
    /**
     * Reads counters into this hierarchy.
     * 
     * @param os
     *            The source from which the data will be read.
     * @param instrumentFactory
     *            Used to create counters on an as needed basis.
     * @param filter
     *            An optional filter, when specified only counters matching the
     *            filter will be processed.
     * @throws IOException
     */
    public void readXML(InputStream is, IInstrumentFactory instrumentFactory,
            Pattern filter) throws IOException, ParserConfigurationException,
            SAXException;

}
