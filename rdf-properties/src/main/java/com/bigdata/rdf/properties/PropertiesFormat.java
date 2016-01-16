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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
/*
 * Created on Jul 25, 2012
 */
package com.bigdata.rdf.properties;

import info.aduna.lang.FileFormat;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Formats for a properties file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PropertiesFormat extends FileFormat implements Iterable<PropertiesFormat> {

    /**
     * All known/registered formats for this class.
     */
    private static final CopyOnWriteArraySet<PropertiesFormat> formats = new CopyOnWriteArraySet<PropertiesFormat>();

    /**
     * A thread-safe iterator that will visit all known formats (declared by
     * {@link Iterable}).
     */
    @Override
    public Iterator<PropertiesFormat> iterator() {
        
        return formats.iterator();
        
    }

    /**
     * Alternative static method signature.
     */
    static public Iterator<PropertiesFormat> getFormats() {
        
        return formats.iterator();
        
    }
    
    /**
     * Text properties file using <code>text/plain</code> and
     * <code>UTF-8</code>.
     */
    public static final PropertiesFormat TEXT = new PropertiesFormat(//
            "text/plain",//
            Arrays.asList("text/plain"),//
            Charset.forName("UTF-8"), //
            Arrays.asList("properties")//
            );

    /**
     * XML properties file using <code>application/xml</code> and
     * <code>UTF-8</code>.
     */
    public static final PropertiesFormat XML = new PropertiesFormat(//
            "application/xml",//
            Arrays.asList("application/xml"),//
            Charset.forName("UTF-8"),// charset
            Arrays.asList("xml")// known-file-extensions
    );

    /**
     * Registers the specified format.
     */
    public static void register(final PropertiesFormat format) {
    
        formats.add(format);
        
    }

    static {
        
        register(TEXT);
        register(XML);
        
    }
    
//    /**
//     * Binary properties file using <code>application/octet-stream</code>
//     */
//    public static final PropertiesFormat BINARY = new PropertiesFormat(//
//            "application/octet-stream",//
//            Arrays.asList("application/octet-stream"),//
//            null,// charset
//            (List) Collections.emptyList()// known-file-extensions
//    );

    /**
     * Creates a new RDFFormat object.
     * 
     * @param name
     *            The name of the RDF file format, e.g. "RDF/XML".
     * @param mimeTypes
     *            The MIME types of the RDF file format, e.g.
     *            <tt>application/rdf+xml</tt> for the RDF/XML file format.
     *            The first item in the list is interpreted as the default
     *            MIME type for the format.
     * @param charset
     *            The default character encoding of the RDF file format.
     *            Specify <tt>null</tt> if not applicable.
     * @param fileExtensions
     *            The RDF format's file extensions, e.g. <tt>rdf</tt> for
     *            RDF/XML files. The first item in the list is interpreted
     *            as the default file extension for the format.
     */
    public PropertiesFormat(final String name,
            final Collection<String> mimeTypes, final Charset charset,
            final Collection<String> fileExtensions) {

        super(name, mimeTypes, charset, fileExtensions);
        
    }

    /**
     * Tries to determine the appropriate file format based on the a MIME
     * type that describes the content type.
     * 
     * @param mimeType
     *        A MIME type, e.g. "text/html".
     * @return An {@link PropertiesFormat} object if the MIME type was recognized, or
     *         <tt>null</tt> otherwise.
     * @see #forMIMEType(String,PropertiesFormat)
     * @see #getMIMETypes()
     */
    public static PropertiesFormat forMIMEType(final String mimeType) {

        return forMIMEType(mimeType, null);
        
    }

    /**
     * Tries to determine the appropriate file format based on the a MIME
     * type that describes the content type. The supplied fallback format will be
     * returned when the MIME type was not recognized.
     * 
     * @param mimeType
     *        A file name.
     * @return An {@link PropertiesFormat} that matches the MIME type, or the fallback format if
     *         the extension was not recognized.
     * @see #forMIMEType(String)
     * @see #getMIMETypes()
     */
    public static PropertiesFormat forMIMEType(String mimeType,
            PropertiesFormat fallback) {

        return matchMIMEType(mimeType, formats/* Iterable<FileFormat> */,
                fallback);
        
    }

}
