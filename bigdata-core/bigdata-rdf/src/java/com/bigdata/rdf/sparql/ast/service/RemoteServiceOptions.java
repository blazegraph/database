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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

/**
 * Configurable options for a remote service end point.
 * 
 * @see RemoteServiceFactoryImpl
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RemoteServiceOptions.java 6068 2012-03-03 21:34:31Z thompsonbry
 *          $
 * 
 *          TODO Add option to indicate that the service supports the ESTCARD
 *          feature ( fast range count for a triple pattern)? The feature should
 *          be disclosed through the service description, but we will need to
 *          publish a unique URI for that feature. Maybe we can get openrdf to
 *          implement the feature as well?
 * 
 *          TODO Add {@link RemoteServiceOptions} options for additional URL
 *          query parameters (defaultGraph, etc), authentication, etc.
 * 
 */
public class RemoteServiceOptions extends ServiceOptionsBase {

//    static private final String DEFAULT_ACCEPT_HEADER;
//    
//    static {
//
//        DEFAULT_ACCEPT_HEADER = //
//                TupleQueryResultFormat.BINARY.getDefaultMIMEType() + ";q=1" + //
//                "," + //
//                TupleQueryResultFormat.SPARQL.getDefaultMIMEType() + ";q=.9"//
//        ;
//
//    }
    
    /**
     * Note: The default is <code>false</code>. This supports use cases where
     * the end points are read/write databases and http caching must be defeated
     * in order to gain access to the most recent committed state of the end
     * point.
     * 
     * @see #isGET()
     */
    private final static boolean DEFAULT_IS_GET = false;
    
    private SPARQLVersion sparqlVersion = SPARQLVersion.SPARQL_11;
    private boolean isGET = DEFAULT_IS_GET;
    private String acceptStr = null;
    

    public RemoteServiceOptions() {

    }

    /**
     * Always returns <code>false</code> since the service is remote.
     */
    @Override
    final public boolean isBigdataNativeService() {
        return false;
    }

    /**
     * Always returns <code>true</code> since the service is remote.
     */
    @Override
    final public boolean isRemoteService() {
        return true;
    }

    @Override
    public boolean isSparql10() {
        return getSPARQLVersion().equals(SPARQLVersion.SPARQL_10);
    }

    /**
     * When <code>true</code>, use GET for query and otherwise use POST (default
     * {@value #DEFAULT_IS_GET}). POST can often handle larger queries than GET
     * due to limits at the HTTP client layer and will defeat http caching and
     * thus provide a current view of the committed state of the SPARQL end
     * point when the end point is a read/write database. However, GET supports
     * HTTP caching and can scale much better when the SPARQL end point is a
     * read-only resource or a read-mostly resource where stale reads are
     * acceptable.
     */
    public boolean isGET() {

        return isGET;
        
    }

    public void setGET(final boolean newValue) {
        
        this.isGET = newValue;
        
    }
    
    /**
     * The <code>Accept</code> header.
     * 
     * @return The header -or- <code>null</code> if the default header should be
     *         used.
     */
    public String getAcceptHeader() {
        
        return acceptStr;
        
    }

    /**
     * Set a non-default <code>Accept</code> header.
     * 
     * @param newValue
     *            The new value -or- <code>null</code> to clear the old value.
     */
    public void setAcceptHeader(final String newValue) {
        
        this.acceptStr = newValue;
        
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{SPARQLVersion=" + getSPARQLVersion());
        sb.append(",GET=" + isGET());
        sb.append(",Accept=" + getAcceptHeader());
        sb.append("}");
        return sb.toString();
    }

	@Override
	public SPARQLVersion getSPARQLVersion() {
		return sparqlVersion;
	}
	
	public void setSPARQLVersion (SPARQLVersion sparqlVersion) {
		if(sparqlVersion == null) {
			throw new IllegalArgumentException("SPARQL version value must not be null");
		}
		this.sparqlVersion = sparqlVersion;
	}

}
