/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import org.openrdf.query.resultio.TupleQueryResultFormat;

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
public class RemoteServiceOptions implements IServiceOptions {

    static private final String DEFAULT_ACCEPT_HEADER;
    
    static {

        DEFAULT_ACCEPT_HEADER = //
                TupleQueryResultFormat.BINARY.getDefaultMIMEType() + ";q=1" + //
                "," + //
                TupleQueryResultFormat.SPARQL.getDefaultMIMEType() + ";q=.9"//
        ;

    }
    
    private boolean isSparql11 = true;
    private boolean isGET = true;
    private String acceptStr = DEFAULT_ACCEPT_HEADER;

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
    public boolean isSparql11() {
        return isSparql11;
    }

    public void setSparql11(final boolean newValue) {
        this.isSparql11 = newValue;
    }

    /**
     * When <code>true</code>, use GET for query. Otherwise use POST. Note that
     * POST can often handle larger queries than GET due to limits at the HTTP
     * client layer.
     */
    public boolean isGET() {
        return isGET;
    }

    /**
     * FIXME Disabled since the {@link RemoteServiceCallImpl} does not support
     * POST yet.
     */
    public void setGET(final boolean newValue) {
        if (true) {
            throw new UnsupportedOperationException();
        }
        this.isGET = newValue;
    }
    
    public String getAcceptHeader() {
        
        return acceptStr;
        
    }

    public void setAcceptHeader(final String newValue) {
        
        if (newValue == null || newValue.length() == 0)
            throw new IllegalArgumentException();
        
        this.acceptStr = newValue;
        
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{sparql11=" + isSparql11());
        sb.append(",GET=" + isGET());
        sb.append(",Accept=" + getAcceptHeader());
        sb.append("}");
        return sb.toString();
    }

}
