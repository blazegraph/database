/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 22, 2010
 */

package com.bigdata.rdf.rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFParserBase;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.bigdata.rawstore.Bytes;

/**
 * A wrapper for an {@link NxParser} which implements the {@link RDFParser}
 * interface. Instances of this class are not thread safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Write some unit tests for this integration.
 */
public class NQuadsParser extends RDFParserBase implements RDFParser  {

    final protected transient static Logger log = Logger
            .getLogger(NQuadsParser.class);

    /**
     * This hook may be used to force the load of this class so it can register
     * the {@link #nquads} {@link RDFFormat} and the parser.
     */
    static public void forceLoad() {

    }
    
    /**
     * The nquads RDF format.
     * <p>
     * The file extension <code>.nq</code> is recommended for N-Quads documents.
     * The media type is <code>text/x-nquads</code> and the encoding is 7-bit
     * US-ASCII. The URI that identifies the N-Quads syntax is
     * <code>http://sw.deri.org/2008/07/n-quads/#n-quads</code>.
     * </p>
     * 
     * @see http://sw.deri.org/2008/07/n-quads/
     */
    static public final RDFFormat nquads;

    /**
     * Register an {@link RDFFormat} for the {@link NxParser} to handle nquads.
     * 
     * @todo These things should be registered automatically using META-INF per
     *       the openrdf javadoc.  For now, use {@link #forceLoad()}.
     */
    static {

        nquads = new RDFFormat(
                // format name.
                "nquads",
                // registered mime types.
                Arrays.asList(new String[] {"text/x-nquads"}), //
                Charset.forName("US-ASCII"), // charset
                // file extensions
                Arrays.asList(new String[]{".nq"}),
                true, // supportsNamespaces,
                true // supportsContexts
        );
        
        // register the nquads format.
        RDFFormat.register(nquads);

        // register the parser factory for nquads.
        RDFParserRegistry.getInstance().add(new RDFParserFactory() {

            public RDFParser getParser() {
                return new NQuadsParser();
            }

            public RDFFormat getRDFFormat() {
                return nquads;
            }

        });
        
    }

    private ValueFactory valueFactory = new ValueFactoryImpl();

    public void setValueFactory(ValueFactory f) {

        if (f == null)
            throw new IllegalArgumentException();

        super.setValueFactory(this.valueFactory = f);

    }
    
    public NQuadsParser() {
        
    }

    public RDFFormat getRDFFormat() {

        return nquads;
        
    }

    public void parse(final InputStream is, final String baseUriIsIgnored)
            throws IOException, RDFParseException, RDFHandlerException {

        final Reader r = new InputStreamReader(is);

        try {

            parse(r, baseUriIsIgnored);

        } finally {

            r.close();

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * {@link NxParser} supports triples, quads, etc. However, this code only
     * handles triples or quads.
     * 
     * @todo NxParser logs on stderr.
     * 
     * @todo NxParser logs and then swallows some errors.
     * 
     * @todo The RDFParser options should be applied to the {@link NxParser}
     *       instance.
     * 
     * @todo {@link NxParser} does not have a constructor which accepts a
     *       {@link Reader}. It has been modified to support this.
     * 
     * @todo Support {@link DatatypeHandling} values.
     * 
     * @todo {@link NxParser} tracks line numbers, so propagate those to RIO.
     * 
     * @todo Verify that this does/does not respect the RIO preseveBlankNodes
     *       option.
     * 
     *       FIXME LEXICON_REFACTOR This is automatically dropping long literals
     *       and logs an warning when it does so. We should provide an option to
     *       allow/disallow them and store them appropriately.
     */
    public void parse(Reader r, String baseUriIsIgnored) throws IOException,
            RDFParseException, RDFHandlerException {

        if (r == null)
            throw new IllegalArgumentException();

        // I doubt that this will do any good.
        setBaseURI(baseUriIsIgnored);

        final RDFHandler handler = getRDFHandler();

        if (handler == null)
            throw new IllegalStateException();

        final ValueFactory f = valueFactory;

        /*
         * The semantics of [strict] indicate whether or not to report errors
         * and continue or stop when an error is encountered.
         */
        final boolean strict = stopAtFirstError();

        /*
         * @todo The semantics of [parseDTs] appears to be whether or not to
         * return a validated instance of a subclass of Literal based on the
         * datatype URI. This is similar to the [datatypeHandling] enum for
         * Sesame (IGNORE, NORMALIZE, VALIDATE). The code uses [parseDTs =
         * false] for now but should respect the three-way distinction made by
         * Sesame.
         */
        final boolean parseDTs = false;

        final NxParser parser = new NxParser(r, strict, parseDTs);

        while (parser.hasNext()) {

            final Node[] nodes = parser.next();

            if (nodes.length != 3 && nodes.length != 4)
                throw new RuntimeException(
                        "Only triples are quads are supported: found n="
                                + nodes.length);

            final Resource s;
            final Value o;
            final URI p;
            final Resource c;

            // s
            if (nodes[0] instanceof org.semanticweb.yars.nx.Resource) {
                s = f.createURI(nodes[0].toString());
            } else if (nodes[0] instanceof org.semanticweb.yars.nx.BNode) {
                s = f.createBNode(nodes[0].toString());
            } else
                throw new RuntimeException();

            // p
            p = f.createURI(nodes[1].toString());

            // o
            if (nodes[2] instanceof org.semanticweb.yars.nx.Resource) {
                o = f.createURI(nodes[2].toString());
            } else if (nodes[2] instanceof org.semanticweb.yars.nx.BNode) {
                o = f.createBNode(nodes[2].toString());
            } else if (nodes[2] instanceof org.semanticweb.yars.nx.Literal) {
                final org.semanticweb.yars.nx.Literal tmp = (org.semanticweb.yars.nx.Literal) nodes[2];
                final int len = tmp.getData().length();
                if (len > (Bytes.kilobyte32 * 64)) {
                    log
                            .warn("Dropping statement with long literal: length="
                                    + len
                                    + (tmp.getDatatype() != null ? ",datatype="
                                            + tmp.getDatatype() : "")
                                    + ", begins="
                                    + tmp
                                            .getData()
                                            .substring(0/* beginIndex */, 100/* endIndex */));
                    continue;
                }
                o = f.createLiteral(nodes[2].toString());
            } else
                throw new RuntimeException();
            
            // c
            if (nodes.length > 3) {
                if (nodes[3] instanceof org.semanticweb.yars.nx.Resource) {
                    c = f.createURI(nodes[3].toString());
                } else if (nodes[3] instanceof org.semanticweb.yars.nx.BNode) {
                    c = f.createBNode(nodes[3].toString());
                } else
                    throw new RuntimeException();
            } else {
                c = null;
            }

            final Statement stmt = f.createStatement(s, p, o, c);

            handler.handleStatement(stmt);

        }

    }

}
