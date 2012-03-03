/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
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
	public static Map<String, String> process(ASTOperationContainer qc)
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
		public Object visit(ASTQName qnameNode, Object data)
			throws VisitorException
		{
			final String qname = qnameNode.getValue();

			final int colonIdx = qname.indexOf(':');
			assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;

			final String prefix = qname.substring(0, colonIdx);
			final String localName = qname.substring(colonIdx + 1);

			// Attempt to resolve the prefix to a namespace.
            String namespace = prefixMap.get(prefix);

            if (namespace == null) {
                /*
                 * Provide silent declaration for some well known namspaces.
                 */
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
                } else {
                    throw new VisitorException("QName '" + qname + "' uses an undefined prefix");
                }
			}

			// Replace the qname node with a new IRI node in the parent node
			final ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
			iriNode.setValue(namespace + localName);
			qnameNode.jjtReplaceWith(iriNode);

			return null;
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

	}
	
}
