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

/**
 * Default implementation of {@link GeoSpatialLiteralSerializer}, translating
 * literals of the form F1#F2#...#Fn to a component string of length n and back.
 * 
 * @author msc
 */
public class GeoSpatialDefaultLiteralSerializer implements GeoSpatialLiteralSerializer {

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

}
