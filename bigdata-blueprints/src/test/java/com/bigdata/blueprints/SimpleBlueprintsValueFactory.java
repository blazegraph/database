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

package com.bigdata.blueprints;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.impl.literal.IPv4AddrIV;

public class SimpleBlueprintsValueFactory extends DefaultBlueprintsValueFactory {

    public static final String ID = "id:";
    
    public static final URI VERTEX = new URIImpl("bigdata:Vertex");
    
    public static final URI EDGE = new URIImpl("bigdata:Edge");
    
    public static final URI TYPE = new URIImpl("bigdata:type");
    
    public static final URI LABEL = new URIImpl("bigdata:label");
    
    public static final SimpleBlueprintsValueFactory INSTANCE =
            new SimpleBlueprintsValueFactory();
    
    public SimpleBlueprintsValueFactory() {
        super(ID, ID, ID, TYPE, VERTEX, EDGE, LABEL);
    }

    /**
     * Override to allow for colons in the id without URLEncoding them.
     */
    @Override
    public URI toPropertyURI(final String property) {
        return toURI(property);
    }

    /**
     * Override to allow for colons in the id without URLEncoding them.
     */
    @Override
    public URI toVertexURI(final Object key) {
        return toURI(key.toString());
    }

    /**
     * Override to allow for colons in the id without URLEncoding them.
     */
    @Override
    public URI toEdgeURI(final Object key) {
        return toURI(key.toString());
    }
    
    public URI toURI(final String id) {
        if (!id.contains(":")) {
            return vf.createURI(ID+tokenizeColons(id));
        } else {
            return vf.createURI(tokenizeColons(id));
        }
    }

    /**
     * URLEncode everything but the colon characters into the URI.
     */
    protected static String tokenizeColons(final String id) {

        try {
            
            final Matcher matcher = IPv4AddrIV.getIPv4Matcher(id);
            if (matcher.matches()) {
                return id;
            }
            
            final List<String> tokens = new LinkedList<String>();

            final StringTokenizer tokenizer = new StringTokenizer(id, ":");
            while (tokenizer.hasMoreTokens()) {
                final String token = tokenizer.nextToken();
                final String encoded = URLEncoder.encode(token, "UTF-8");
                tokens.add(encoded);
            }

            final StringBuilder sb = new StringBuilder();
            for (String token : tokens) {
                sb.append(token).append(':');
            }
            sb.setLength(sb.length() - 1);

            return sb.toString();

        } catch (UnsupportedEncodingException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Gracefully handle round-tripping the colons.
     */
    @Override
    public String fromURI(final URI uri) {

        if (uri == null) {
            throw new IllegalArgumentException();
        }

        final String s = uri.stringValue();

        final String id;
        if (s.startsWith(ID)) {
            id = s.substring(ID.length());
        } else {
            id = s;
        }

        try {
            return URLDecoder.decode(id, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }

}
