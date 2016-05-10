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
package com.bigdata.rdf.sparql.ast.eval.service;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.service.geospatial.GeoSpatialDefaultLiteralSerializer;

/**
 * Test serializer for WKT literals of the form Point(lat,lon).
 * Note that this is a dummy implementation for tests only,
 * assuming there are no whitespaces contained in the literal, etc.
 * 
 * @author msc
 */
public class GeoSpatialDummyLiteralSerializer extends GeoSpatialDefaultLiteralSerializer {

    // we do use the default toComponent() and fromComponent() methods defined in GeoSpatialDefaultLiteralSerializer,
    // but override all the serialization methods in this class (which is the focus of the test cases)
    
    @Override
    public IV<?,?> serializeLocation(
        final BigdataValueFactory vf, final Object latitude, final Object longitude) {

        return DummyConstantNode.toDummyIV(vf.createLiteral("Location(" + concat(latitude, longitude) + ")"));

        
    }

    @Override
    public IV<?,?> serializeLocationAndTime(
        final BigdataValueFactory vf, final Object latitude, 
        final Object longitude, final Object time) {

        return DummyConstantNode.toDummyIV(vf.createLiteral("LocationAndTime(" + concat(latitude, longitude, time) + ")"));

    }

    @Override
    public IV<?,?> serializeTime(final BigdataValueFactory vf, final Object time) {
        return DummyConstantNode.toDummyIV(vf.createLiteral("Time(" + time.toString() + ")"));
    }

    @Override
    public IV<?,?> serializeLatitude(final BigdataValueFactory vf, final Object latitude) {
        return DummyConstantNode.toDummyIV(vf.createLiteral("Lat(" + latitude.toString() + ")"));
    }

    @Override
    public IV<?,?> serializeLongitude(final BigdataValueFactory vf, final Object longitude) {
        return DummyConstantNode.toDummyIV(vf.createLiteral("Lon(" + longitude.toString() + ")"));
    }

    @Override
    public IV<?,?> serializeCoordSystem(final BigdataValueFactory vf, final Object coordinateSystem) {
        return DummyConstantNode.toDummyIV(vf.createLiteral("CoordSystem(" + coordinateSystem.toString() + ")"));
    }

    @Override
    public IV<?,?> serializeCustomFields(final BigdataValueFactory vf, final Object... customFields) {
        return DummyConstantNode.toDummyIV(vf.createLiteral("CustomFields(" + concat(customFields) + ")"));
    }
    
    protected String concat(final Object... args) {
      
        final StringBuffer buf = new StringBuffer();
        
        for (int i=0; i<args.length; i++) {
               
            if (i>0)
                buf.append("-");
               
            buf.append(args[i].toString());
        }
        
        return buf.toString();
    }
    
}
