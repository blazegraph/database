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

package com.bigdata.rdf.rio.nquads;

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
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.RDFParserBase;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.bigdata.rdf.internal.ILexiconConfiguration;

/**
 * A wrapper for an {@link NxParser} which implements the {@link RDFParser}
 * interface. Instances of this class are not thread safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Add {@link RDFWriter} for NQUADS.
 */
public class NQuadsParser extends RDFParserBase implements RDFParser  {

    final protected transient static Logger log = Logger
            .getLogger(NQuadsParser.class);

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
	 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/439">
	 *      Class loader problems </a>
	 * @see <a href="http://www.openrdf.org/issues/browse/SES-802"> Please add
	 *      support for NQuads format </a>
	 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/559"> Use
	 *      RDFFormat.NQUADS as the format identifier for the NQuads parser </a>
	 */
    static {

        nquads = new RDFFormat(
                // format name.
                "N-Quads",
                // registered mime types.
                Arrays.asList(new String[] {"text/x-nquads"}), //
                Charset.forName("US-ASCII"), // charset
                // file extensions
                Arrays.asList(new String[]{"nq"}),
                false, // supportsNamespaces,
                true // supportsContexts
        );
        
        // register the nquads format.
        RDFFormat.register(nquads);

//        // register the parser factory for nquads.
//        RDFParserRegistry.getInstance().add(new RDFParserFactory() {
//
//            public RDFParser getParser() {
//                return new NQuadsParser();
//            }
//
//            public RDFFormat getRDFFormat() {
//                return nquads;
//            }
//
//        });
        
    }

    private ValueFactory valueFactory = new ValueFactoryImpl();

    public void setValueFactory(final ValueFactory f) {

        if (f == null)
            throw new IllegalArgumentException();

        super.setValueFactory(this.valueFactory = f);

    }
    
    public NQuadsParser() {
        
    }

    public RDFFormat getRDFFormat() {

        return nquads;
        
    }

    private URI asURI(final Node node, final String baseUri) {

        final org.semanticweb.yars.nx.Resource r = (org.semanticweb.yars.nx.Resource) node;

        String uriString = r.toString();

        /*
         * Note: This hack makes it possible to load data where blank node
         * IDs were mistakenly emitted as URIs <node...>.  This was true of
         * one version of the tbl data sets at:
         * 
         * http://km.aifb.kit.edu/projects/btc-2012/
         * 
         * However, the data sets are being fixed.  You can hack the code
         * here if you encounter this problem.
         */
        final URI uri;
        if (false && uriString.indexOf(':') == -1) {
            uri = valueFactory.createURI(baseUri + "#" + uriString);
        } else {
            uri = valueFactory.createURI(uriString);
        }
        return uri;
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
     *       FIXME This is automatically dropping long literals and logs an
     *       warning when it does so. We should provide an option to
     *       allow/disallow long literals in the {@link ILexiconConfiguration}
     *       and store them appropriately.
     */
    public void parse(final Reader r, final String baseUri)
            throws IOException, RDFParseException, RDFHandlerException {

        if (r == null)
            throw new IllegalArgumentException();

        // I doubt that this will do any good.
        setBaseURI(baseUri);

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
                        "Only triples or quads are supported: found n="
                                + nodes.length);

            final Resource s;
            final Value o;
            final URI p;
            final Resource c;

            // s
            if (nodes[0] instanceof org.semanticweb.yars.nx.Resource) {
//                s = f.createURI(nodes[0].toString());
                s = asURI(nodes[0],baseUri);
            } else if (nodes[0] instanceof org.semanticweb.yars.nx.BNode) {
                s = f.createBNode(nodes[0].toString());
            } else
                throw new RuntimeException();

            // p
//            p = f.createURI(nodes[1].toString());
            p = asURI(nodes[1],baseUri);

            // o
            if (nodes[2] instanceof org.semanticweb.yars.nx.Resource) {
                o = asURI(nodes[2],baseUri);
            } else if (nodes[2] instanceof org.semanticweb.yars.nx.BNode) {
                o = f.createBNode(nodes[2].toString());
            } else if (nodes[2] instanceof org.semanticweb.yars.nx.Literal) {
//                final org.semanticweb.yars.nx.Literal tmp = (org.semanticweb.yars.nx.Literal) nodes[2];
//                final int len = tmp.getData().length();
//                if (len > (Bytes.kilobyte32 * 64)) {
//                    log
//                            .warn("Dropping statement with long literal: length="
//                                    + len
//                                    + (tmp.getDatatype() != null ? ",datatype="
//                                            + tmp.getDatatype() : "")
//                                    + ", begins="
//                                    + tmp
//                                            .getData()
//                                            .substring(0/* beginIndex */, 100/* endIndex */));
//                    continue;
//                }
                o = f.createLiteral(nodes[2].toString());
            } else
                throw new RuntimeException();
            
            // c
            if (nodes.length > 3) {
                if (nodes[3] instanceof org.semanticweb.yars.nx.Resource) {
//                    c = f.createURI(nodes[3].toString());
                    c = asURI(nodes[3],baseUri);
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
