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
 * Created on March 02, 2016
 */
package com.bigdata.service;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.service.geospatial.GeoSpatial;

/**
 * Default implementation of {@link IGeoSpatialLiteralSerializer}, translating
 * literals of the form F1#F2#...#Fn to a component string of length n and back.
 * 
 * @author msc
 */
public class GeoSpatialDefaultLiteralSerializer implements IGeoSpatialLiteralSerializer {

    private static final String COMPONENT_SEPARATOR = "#";
    
    @Override
    public String[] toComponents(String literalString) {
        if (literalString==null) {
            return new String[0];
        }
        
        return literalString.split(COMPONENT_SEPARATOR);
    }

    @Override
    public String fromComponents(Object[] components) {
        
        if (components==null)
            return "";
        
        final StringBuffer buf = new StringBuffer();
        for (int i=0; i<components.length; i++) {
           
           if (i>0)
              buf.append(COMPONENT_SEPARATOR);
           
           buf.append(components[i]);
           
        }
        
        return buf.toString();
    }

    @Override
    public IV<?,?> serializeLocation(
        final BigdataValueFactory vf, final Object latitude, final Object longitude) {

        return toSeparatedString(vf, latitude, longitude);
    }

    @Override
    public IV<?,?> serializeLocationAndTime(
        final BigdataValueFactory vf, final Object latitude, 
        final Object longitude, final Object time) {

        return toSeparatedString(vf, latitude, longitude, time);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public IV<?,?> serializeTime(final BigdataValueFactory vf, final Object time) {
        return new XSDNumericIV((Long)time);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public IV<?,?> serializeLatitude(final BigdataValueFactory vf, final Object latitude) {
        return new XSDNumericIV((Double)latitude);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public IV<?,?> serializeLongitude(final BigdataValueFactory vf, final Object longitude) {
        return new XSDNumericIV((Double)longitude);
    }

    @Override
    public IV<?,?> serializeCoordSystem(final BigdataValueFactory vf, final Object coordinateSystem) {
        return toSeparatedString(vf, coordinateSystem);
    }

    @Override
    public IV<?,?> serializeCustomFields(final BigdataValueFactory vf, final Object... customFields) {
        return toSeparatedString(vf, customFields);
    }
    
    /**
     * Converts the input passed via args into string using its toString() method, 
     * separating the components via {GeoSpatial#CUSTOM_FIELDS_SEPARATOR}.
     */
    protected IV<?,?> toSeparatedString(final BigdataValueFactory vf, final Object... args) {
      
        final StringBuffer buf = new StringBuffer();
        
        for (int i=0; i<args.length; i++) {
               
            if (i>0)
                buf.append(GeoSpatial.CUSTOM_FIELDS_SEPARATOR);
               
            buf.append(args[i].toString());
        }
        
        return  DummyConstantNode.toDummyIV(vf.createLiteral(buf.toString()));
    }
}
