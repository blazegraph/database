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
package com.bigdata.rdf.sail.webapp;

import java.util.Collection;
import java.util.Collections;

import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.resultio.QueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.WriterConfig;

public class NopTupleQueryResultWriter extends TupleQueryResultHandlerBase implements TupleQueryResultWriter {

    @Override
    public QueryResultFormat getQueryResultFormat() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TupleQueryResultFormat getTupleQueryResultFormat() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public WriterConfig getWriterConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<RioSetting<?>> getSupportedSettings() {
        return Collections.emptySet();
    }

    @Override
    public void handleNamespace(String arg0, String arg1) throws QueryResultHandlerException {
    }

    @Override
    public void handleStylesheet(String arg0) throws QueryResultHandlerException {
    }

    @Override
    public void setWriterConfig(WriterConfig arg0) {
    }

    @Override
    public void startDocument() throws QueryResultHandlerException {
    }

    @Override
    public void startHeader() throws QueryResultHandlerException {
    }

    @Override
    public void endHeader() throws QueryResultHandlerException {
    }

}
