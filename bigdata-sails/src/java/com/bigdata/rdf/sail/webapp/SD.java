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
 * Created on Mar 2, 2012
 */

package com.bigdata.rdf.sail.webapp;

import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.axioms.Axioms;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.axioms.RdfsAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * SPARQL 1.1 Service Description vocabulary class.
 * 
 * @see <a href="http://www.w3.org/TR/sparql11-service-description/"> SPARQL 1.1
 *      Service Description </a>
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/500
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SD {

    static public final String NS = "http://www.w3.org/ns/sparql-service-description#";

    static public final URI Service = new URIImpl(NS + "Service");

    static public final URI endpoint = new URIImpl(NS + "endpoint");

    /**
     * <pre>
     * <sd:supportedLanguage rdf:resource="http://www.w3.org/ns/sparql-service-description#SPARQL11Query"/>
     * </pre>
     */
    static public final URI supportedLanguage = new URIImpl(NS
            + "supportedLanguage");

    static public final URI SPARQL10Query = new URIImpl(NS + "SPARQL10Query");

    static public final URI SPARQL11Query = new URIImpl(NS + "SPARQL11Query");

    static public final URI SPARQL11Update = new URIImpl(NS + "SPARQL11Update");

    /**
     * Relates an instance of {@link #Service} to a format that is supported for
     * serializing query results. URIs for commonly used serialization formats
     * are defined by Unique URIs for File Formats. For formats that do not have
     * an existing URI, the <a href="http://www.w3.org/ns/formats/media_type">
     * media_type </a> and <a
     * href="http://www.w3.org/ns/formats/preferred_suffix"> preferred_suffix
     * </a> properties defined in that document SHOULD be used to describe the
     * format.
     * 
     * @see <a href="http://www.w3.org/ns/formats/"> Unique URIs for File
     *      Formats </a>
     */
    //    *<pre>
//   * <sd:resultFormat rdf:resource="http://www.w3.org/ns/formats/RDF_XML"/>
//   * <sd:resultFormat rdf:resource="http://www.w3.org/ns/formats/Turtle"/>
//   * </pre>
    static public final URI resultFormat = new URIImpl(NS + "resultFormat");

    /**
     * Relates an instance of sd:Service to a format that is supported for
     * parsing RDF input; for example, via a SPARQL 1.1 Update LOAD statement,
     * or when URIs are dereferenced in FROM/FROM NAMED/USING/USING NAMED
     * clauses (see also sd:DereferencesURIs below).
     * <p>
     * URIs for commonly used serialization formats are defined by Unique URIs
     * for File Formats. For formats that do not have an existing URI, the <a
     * href="http://www.w3.org/ns/formats/media_type"> media_type </a> and <a
     * href="http://www.w3.org/ns/formats/preferred_suffix"> preferred_suffix
     * </a> properties defined in that document SHOULD be used to describe the
     * format.
     * 
     * @see <a href="http://www.w3.org/ns/formats/"> Unique URIs for File
     *      Formats </a>
     */
    static public final URI inputFormat = new URIImpl(NS + "inputFormat");

    /**
     * <pre>
     * <sd:feature rdf:resource="http://www.w3.org/ns/sparql-service-description#DereferencesURIs"/>
     * </pre>
     */
    static public final URI feature = new URIImpl(NS + "feature");
    static public final URI DereferencesURIs = new URIImpl(NS + "DereferencesURIs");
    static public final URI UnionDefaultGraph = new URIImpl(NS + "UnionDefaultGraph");
    static public final URI RequiresDataset = new URIImpl(NS + "RequiresDataset");
    static public final URI EmptyGraphs = new URIImpl(NS + "EmptyGraphs");
    static public final URI BasicFederatedQuery = new URIImpl(NS + "BasicFederatedQuery");
    
    /**
     * <pre>
     * http://www.w3.org/ns/entailment/OWL-RDF-Based
     * http://www.w3.org/ns/entailment/RDFS
     * http://www.w3.org/ns/owl-profile/RL
     * </pre>
     * 
     * @see <a href="http://www.w3.org/ns/entailment/"> Unique URIs for Semantic
     *      Web Entailment Regimes [ENTAILMENT] (members of the class
     *      sd:EntailmentRegime usable with the properties
     *      sd:defaultEntailmentRegime and sd:entailmentRegime) </a>
     *      
     * @see <a href="http://www.w3.org/ns/owl-profile/"> Unique URIs for OWL 2
     *      Profiles [OWL2PROF] (members of the class sd:EntailmentProfile
     *      usable with the properties sd:defaultSupportedEntailmentProfile and
     *      sd:supportedEntailmentProfile) </a>
     */
    static public final URI defaultEntailmentRegime = new URIImpl(NS
            + "defaultEntailmentRegime");

    static public final URI entailmentRegime = new URIImpl(NS
            + "entailmentRegime");

    static public final URI supportedEntailmentProfile = new URIImpl(NS
            + "supportedEntailmentProfile");

    static public final URI extensionFunction = new URIImpl(NS
            + "extensionFunction");

    /*
     * Entailment regimes.
     */
    
    /**
     * Simple Entailment
     * 
     * @see http://www.w3.org/ns/entailment/Simple
     */
    static public final URI simpleEntailment = new URIImpl(
            "http://www.w3.org/ns/entailment/Simple");

    /**
     * RDF Entailment
     * 
     * @see http://www.w3.org/ns/entailment/RDF
     */
    static public final URI rdfEntailment = new URIImpl(
            "http://www.w3.org/ns/entailment/RDF");

    /**
     * RDFS Entailment
     * 
     * @see http://www.w3.org/ns/entailment/RDFS
     */
    static public final URI rdfsEntailment = new URIImpl(
            "http://www.w3.org/ns/entailment/RDFS");
    
    static public final URI Function = new URIImpl(NS + "Function");
    static public final URI defaultDataset = new URIImpl(NS + "defaultDataset");
    static public final URI Dataset = new URIImpl(NS + "Dataset");
    static public final URI defaultGraph = new URIImpl(NS + "defaultGraph");
    static public final URI Graph = new URIImpl(NS + "Graph");
    static public final URI namedGraph = new URIImpl(NS + "namedGraph");
    static public final URI NamedGraph = new URIImpl(NS + "NamedGraph");
    static public final URI name = new URIImpl(NS + "name");

    /**
     * TODO Move VOID vocabulary to its own class.
     * 
     * <pre>
     * <sd:Graph>
     *             <void:triples rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">100</void:triples>
     *           </sd:Graph>
     * </pre>
     */
    static public final String NS_VOID = "http://rdfs.org/ns/void#";

    /*
     * RDF data
     * 
     * TODO RDFa: http://www.w3.org/ns/formats/RDFa
     */
    

    /**
     * Unique URI for RDF/XML
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI RDFXML = new URIImpl(
            "http://www.w3.org/ns/formats/RDF_XML");
    
    /**
     * Unique URI for NTRIPLES
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI NTRIPLES = new URIImpl(
            "http://www.w3.org/ns/formats/N-Triples");

    /**
     * Unique URI for TURTLE
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI TURTLE = new URIImpl(
            "http://www.w3.org/ns/formats/Turtle");

    /**
     * Unique URI for N3.
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI N3 = new URIImpl("http://www.w3.org/ns/formats/N3");

    // /**
    // * TODO The TriX file format.
    // */
    // public static final RDFFormat TRIX = new RDFFormat("TriX",
    // "application/trix", Charset.forName("UTF-8"),
    // Arrays.asList("xml", "trix"), false, true);

    /**
     * The <a
     * href="http://www.wiwiss.fu-berlin.de/suhl/bizer/TriG/Spec/">TriG</a> file
     * format.
     */
    static public final URI TRIG = new URIImpl(
            "http://www.wiwiss.fu-berlin.de/suhl/bizer/TriG/Spec/");

    // /**
    // * TODO A binary RDF format (openrdf)
    // *
    // * @see http://www.openrdf.org/issues/browse/RIO-79 (Request for unique
    // URI)
    // */
    // public static final RDFFormat BINARY = new RDFFormat("BinaryRDF",
    // "application/x-binary-rdf", null,
    // "brf", true, true);

    /**
     * The URI that identifies the N-Quads syntax is
     * <code>http://sw.deri.org/2008/07/n-quads/#n-quads</code>.
     * 
     * @see http://sw.deri.org/2008/07/n-quads/
     */
    static public final URI NQUADS = new URIImpl(
            "http://sw.deri.org/2008/07/n-quads/#n-quads");
    
    /*
     * SPARQL results
     */

    /**
     * Unique URI for SPARQL Results in XML
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI SPARQL_RESULTS_XML = new URIImpl(
            "http://www.w3.org/ns/formats/SPARQL_Results_XML");

    /**
     * Unique URI for SPARQL Results in JSON
     * 
     * @see http://www.w3.org/ns/formats/
     * 
     *      TODO Does openrdf support this yet?
     */
    static public final URI SPARQL_RESULTS_JSON = new URIImpl(
            "http://www.w3.org/ns/formats/SPARQL_Results_JSON");

    /**
     * Unique URI for SPARQL Results in CSV
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI SPARQL_RESULTS_CSV = new URIImpl(
            "http://www.w3.org/ns/formats/SPARQL_Results_CSV");

    /**
     * Unique URI for SPARQL Results in TSV
     * 
     * @see http://www.w3.org/ns/formats/
     */
    static public final URI SPARQL_RESULTS_TSV = new URIImpl(
            "http://www.w3.org/ns/formats/SPARQL_Results_TSV");

    // TODO openrdf binary format.
//    /**
//     * Unique URI for SPARQL Results in TSV
//     * 
//     * @see TupleQueryResultFormat#BINARY
//     * 
//     * @see http://www.openrdf.org/issues/browse/RIO-79 (Request for unique URI) 
//     */
//    static public final URI SPARQL_RESULTS_OPENRDF_BINARY = new URIImpl(
//            "http://www.w3.org/ns/formats/SPARQL_Results_TSV");

    /**
     * Collect various information, building up a service description graph.
     * 
     * TODO I am disinclined to enumerate all named and default graphs when
     * there is a GET against the SPARQL end point. That can be WAY too much
     * information.
     */
    public static Graph describeService(final AbstractTripleStore tripleStore) {

        final Graph g = new GraphImpl();

        /*
         * Supported Query Languages
         */
        g.add(SD.Service, SD.supportedLanguage, SD.SPARQL10Query);
        g.add(SD.Service, SD.supportedLanguage, SD.SPARQL11Query);
        /*
         * TODO Uncomment when implemented.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/448 (SPARQL 1.1
         * UPDATE)
         */
        // g.add(f.createStatement(SD.Service, SD.supportedLanguage,
        // SD.SPARQL11Update));

        /*
         * RDF and SPARQL Formats.
         * 
         * @see http://www.openrdf.org/issues/browse/RIO-79 (Request for unique
         * URIs)
         * 
         * TODO Add an explict declation for SIDS mode data interchange?
         */

        // InputFormats
        {
            g.add(SD.Service, SD.inputFormat, SD.RDFXML);
            g.add(SD.Service, SD.inputFormat, SD.NTRIPLES);
            g.add(SD.Service, SD.inputFormat, SD.TURTLE);
            g.add(SD.Service, SD.inputFormat, SD.N3);
            // g.add(SD.Service, SD.inputFormat, SD.TRIX); // TODO TRIX
            g.add(SD.Service, SD.inputFormat, SD.TRIG);
            // g.add(SD.Service, SD.inputFormat, SD.BINARY); // TODO BINARY
            g.add(SD.Service, SD.inputFormat, SD.NQUADS);

            g.add(SD.Service, SD.inputFormat, SD.SPARQL_RESULTS_XML);
            g.add(SD.Service, SD.inputFormat, SD.SPARQL_RESULTS_JSON);
            g.add(SD.Service, SD.inputFormat, SD.SPARQL_RESULTS_CSV);
            g.add(SD.Service, SD.inputFormat, SD.SPARQL_RESULTS_TSV);
            // g.add(SD.Service, SD.inputFormat,
            // SD.SPARQL_RESULTS_OPENRDF_BINARY);

        }

        // ResultFormats
        {
            g.add(SD.Service, SD.resultFormat, SD.RDFXML);
            g.add(SD.Service, SD.resultFormat, SD.NTRIPLES);
            g.add(SD.Service, SD.resultFormat, SD.TURTLE);
            g.add(SD.Service, SD.resultFormat, SD.N3);
            // g.add(SD.Service, SD.resultFormat, SD.TRIX); // TODO TRIX
            g.add(SD.Service, SD.resultFormat, SD.TRIG);
            // g.add(SD.Service, SD.resultFormat, SD.BINARY); // TODO BINARY
            // g.add(SD.Service, SD.resultFormat, SD.NQUADS); // TODO NQuads
            // writer

            g.add(SD.Service, SD.resultFormat, SD.SPARQL_RESULTS_XML);
            g.add(SD.Service, SD.resultFormat, SD.SPARQL_RESULTS_JSON);
            g.add(SD.Service, SD.resultFormat, SD.SPARQL_RESULTS_CSV);
            g.add(SD.Service, SD.resultFormat, SD.SPARQL_RESULTS_TSV);
            // g.add(SD.Service, SD.resultFormat,
            // SD.SPARQL_RESULTS_OPENRDF_BINARY);
        }

        /*
         * TODO Report out the database mode {triples, provenance, or quads} and
         * the entailment regieme for that mode (only for triples or quads at
         * this point). Report out when truth maintenance is enabled and whether
         * or not the full text index is enabled.
         * 
         * TODO sd:languageExtension or sd:feature could be used for query
         * hints, NAMED SUBQUERY, etc.
         */

        if (tripleStore.isQuads()) {

            g.add(SD.Service, SD.feature, SD.UnionDefaultGraph);

        } else {

            /*
             * TODO The Axioms interface could self-report this.
             */
            final URI entailmentRegime;
            final Axioms axioms = tripleStore.getAxioms();
            if (axioms == null || axioms instanceof NoAxioms) {
                entailmentRegime = SD.simpleEntailment;
            } else if (axioms instanceof OwlAxioms) {
                // TODO This is really the RDFS+ entailment regime.
                entailmentRegime = SD.rdfsEntailment;
            } else if (axioms instanceof RdfsAxioms) {
                entailmentRegime = SD.rdfsEntailment;
            } else {
                // Unknown.
                entailmentRegime = null;
            }
            if (entailmentRegime != null)
                g.add(SD.Service, SD.entailmentRegime, entailmentRegime);

        }

        g.add(SD.Service, SD.feature, SD.BasicFederatedQuery);

        return g;
    }

}
