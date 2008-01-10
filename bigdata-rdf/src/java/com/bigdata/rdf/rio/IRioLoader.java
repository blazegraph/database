/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 27, 2007
 */

package com.bigdata.rdf.rio;

import java.io.Reader;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

/**
 * Interface for parsing RDF data using the Sesame RIO parser.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRioLoader {
    
    public static Logger log = Logger.getLogger(IRioLoader.class);

    public long getStatementsAdded();
    
    public long getInsertTime();
    
    public long getInsertRate();

    /**
     * Register a listener.
     * 
     * @param l
     *            The listener.
     */
    public void addRioLoaderListener( RioLoaderListener l );
    
    /**
     * Remove a listener.
     * 
     * @param l
     *            The listener.
     */
    public void removeRioLoaderListener( RioLoaderListener l );

    /**
     * Parse RDF data.
     * 
     * @param reader
     *            The source from which the data will be read.
     * @param baseURL
     *            The base URL for those data.
     * @throws Exception
     */
    public void loadRdf(Reader reader, String baseURL, RDFFormat rdfFormat,
            boolean verify) throws Exception;

    // public void loadRdf( InputStream is, String baseURI ) throws Exception;
    
}
