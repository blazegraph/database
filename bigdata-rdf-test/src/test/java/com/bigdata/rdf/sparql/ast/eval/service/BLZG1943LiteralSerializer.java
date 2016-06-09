package com.bigdata.rdf.sparql.ast.eval.service;

import com.bigdata.service.geospatial.GeoSpatialDefaultLiteralSerializer;


/**
 * Serializer class for BLZG-1943 test case. Sample code provided 
 * via https://jira.blazegraph.com/browse/BLZG-1943 by Mark Hale.
 * 
 * @author msc
 */
public class BLZG1943LiteralSerializer extends GeoSpatialDefaultLiteralSerializer {
    
    private static final long serialVersionUID = 9079111874531955526L;

    public String fromComponents(Object[] components)
    {
        return "POINT("+components[0]+" "+components[1]+")";
    }

    public String[] toComponents(String str)
    {
        int startPos = "POINT(".length();
        int endPos = str.length() - 1;
        int sep = str.indexOf(' ', startPos);
        return new String[] {str.substring(startPos, sep), str.substring(sep+1, endPos)};
    }
}
