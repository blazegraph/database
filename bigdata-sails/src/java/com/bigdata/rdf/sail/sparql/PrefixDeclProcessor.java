/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTOperationContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTPrefixDecl;
import com.bigdata.rdf.sail.sparql.ast.ASTQName;
import com.bigdata.rdf.sail.sparql.ast.ASTServiceGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilderTreeConstants;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Processes the prefix declarations in a SPARQL query model.
 *
 * @author Arjohn Kampman 
 * @openrdf
 */
public class PrefixDeclProcessor {

    /**
     * Processes prefix declarations in queries. This method collects all
     * prefixes that are declared in the supplied query, verifies that prefixes
     * are not redefined and replaces any {@link ASTQName} nodes in the query
     * with equivalent {@link ASTIRI} nodes.
     * 
     * @param qc
     *        The query that needs to be processed.
     * @return A map containing the prefixes that are declared in the query (key)
     *         and the namespace they map to (value).
     * @throws MalformedQueryException
     *         If the query contains redefined prefixes or qnames that use
     *         undefined prefixes.
     */
    public static Map<String, String> process(final ASTOperationContainer qc)
        throws MalformedQueryException
    {
        List<ASTPrefixDecl> prefixDeclList = qc.getPrefixDeclList();

        // Build a prefix --> IRI map
        Map<String, String> prefixMap = new LinkedHashMap<String, String>();
        for (ASTPrefixDecl prefixDecl : prefixDeclList) {
            String prefix = prefixDecl.getPrefix();
            String iri = prefixDecl.getIRI().getValue();

            if (prefixMap.containsKey(prefix)) {
                throw new MalformedQueryException("Multiple prefix declarations for prefix '" + prefix + "'");
            }

            prefixMap.put(prefix, iri);
        }

        QNameProcessor visitor = new QNameProcessor(prefixMap);
        try {
            qc.jjtAccept(visitor, null);
        }
        catch (VisitorException e) {
            throw new MalformedQueryException(e);
        }

        return prefixMap;
    }

    private static class QNameProcessor extends ASTVisitorBase {

        private Map<String, String> prefixMap;

        public QNameProcessor(Map<String, String> prefixMap) {
            this.prefixMap = prefixMap;
        }

        @Override
        public Object visit(final ASTQName qnameNode, final Object data)
            throws VisitorException
        {
            final String qname = qnameNode.getValue();

            final int colonIdx = qname.indexOf(':');
            assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;

            final String prefix = qname.substring(0, colonIdx);
            final String localName0 = qname.substring(colonIdx + 1);
            final String localName = processEscapesAndHex(localName0);

            // Attempt to resolve the prefix to a namespace.
            String namespace = prefixMap.get(prefix);

            if (namespace == null) {
                // Check for well-known namespace prefix (implicit decl).
                if ((namespace = checkForWellKnownNamespacePrefix(prefix)) == null) {
                    throw new VisitorException("QName '" + qname
                            + "' uses an undefined prefix");
                }
            }

            // Replace the qname node with a new IRI node in the parent node
            final ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
            iriNode.setValue(namespace + localName);
            qnameNode.jjtReplaceWith(iriNode);

            return null;
        }
    
        private static final Pattern hexPattern = Pattern.compile(
                "([^\\\\]|^)(%[A-F\\d][A-F\\d])", Pattern.CASE_INSENSITIVE);

        private static final Pattern escapedCharPattern = Pattern
                .compile("\\\\[_~\\.\\-!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:\\/\\?#\\@\\%]");

        /**
         * Buffer is reused for the unencode and unescape of each localName
         * within the scope of a given Query.
         */
        private final StringBuffer tmp = new StringBuffer();
        
        private String processEscapesAndHex(final String localName) {
            
            // first process hex-encoded chars.
            final String unencodedStr;
            {
                final StringBuffer unencoded = tmp; tmp.setLength(0);//new StringBuffer();
                final Matcher m = hexPattern.matcher(localName);
                boolean result = m.find();
                while (result) {
                    // we match the previous char because we need to be sure we are not processing an escaped % char rather than
                    // an actual hex encoding, for example: 'foo\%bar'.
                    final String previousChar = m.group(1);
                    final String encoded = m.group(2);
    
                    final int codePoint = Integer.parseInt(encoded.substring(1), 16);
                    final String decoded = String.valueOf( Character.toChars(codePoint));
                    
                    m.appendReplacement(unencoded, previousChar + decoded);
                    result = m.find();
                }
                m.appendTail(unencoded);
                unencodedStr = unencoded.toString();
            }

            // then process escaped special chars.
            final String unescapedStr;
            {
                final StringBuffer unescaped = tmp; tmp.setLength(0);// new StringBuffer();
                final Matcher m = escapedCharPattern.matcher(unencodedStr);
                boolean result = m.find();
                while (result) {
                    final String escaped = m.group();
                    m.appendReplacement(unescaped, escaped.substring(1));
                    result = m.find();
                }
                m.appendTail(unescaped);
                unescapedStr = unescaped.toString();
   }
            
            return unescapedStr;
        }

        /**
         * Attach the prefix declarations to the SERVICE node.
         */
        @Override
        public Object visit(final ASTServiceGraphPattern node, Object data)
            throws VisitorException
        {
            node.setPrefixDeclarations(prefixMap);
            return super.visit(node, data);
        }

        /**
         * Provide silent declaration for some well known namspaces.
         */
        private String checkForWellKnownNamespacePrefix(final String prefix) {
            final String namespace;
            if (prefix.equals("bd")) {
                prefixMap.put("bd", namespace = BD.NAMESPACE);
            } else if (prefix.equals("hint")) {
                prefixMap.put("hint", namespace = QueryHints.NAMESPACE);
            } else if (prefix.equals("rdf")) {
                prefixMap.put("rdf", namespace = RDF.NAMESPACE);
            } else if (prefix.equals("rdfs")) {
                prefixMap.put("rdfs", namespace = RDFS.NAMESPACE);
            } else if (prefix.equals("xsd")) {
                prefixMap.put("xsd", namespace = XSD.NAMESPACE);
            } else if (prefix.equals("foaf")) {
                prefixMap.put("foaf", namespace = FOAFVocabularyDecl.NAMESPACE);
            } else if (prefix.equals("fn")) { // XPath Functions.
                prefixMap.put("fn", namespace = FN.NAMESPACE);
            } else if (prefix.equals("owl")) {
                prefixMap.put("owl", namespace = OWL.NAMESPACE);
            } else if (prefix.equals("sesame")) {
                prefixMap.put("sesame", namespace = SESAME.NAMESPACE);
            } else {
                // Unknown
                namespace = null;
            }
            return namespace;
        }

    }

}
