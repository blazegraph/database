/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.graph.impl.bd.GASService;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.sparql.ast.ASTDeleteData;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTInsertData;
import com.bigdata.rdf.sail.sparql.ast.ASTOperationContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTPrefixDecl;
import com.bigdata.rdf.sail.sparql.ast.ASTQName;
import com.bigdata.rdf.sail.sparql.ast.ASTServiceGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTUnparsedQuadDataBlock;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilderTreeConstants;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BDS;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Processes the prefix declarations in a SPARQL query model.
 *
 * @author Arjohn Kampman 
 * @openrdf
 */
public class PrefixDeclProcessor {
	
    private static final Logger log = Logger.getLogger(PrefixDeclProcessor.class);


    public static final Map<String,String> defaultDecls =
            new LinkedHashMap<String, String>();
    
    public static interface Options {
        
        /**
         * 
         * This optional property defines the path to a file containing the prefix declarations.  
         * 
         * Example file contents are shown below.
         * 
         * <code>
         * PREFIX wdref: <http://www.wikidata.org/reference/>
		 * PREFIX wikibase: <http://wikiba.se/ontology#>
         * </code>
         * 
         * This should be passed as a Java Property as:
         * 
         * <code>
         * -Dcom.bigdata.rdf.sail.sparql.PrefixDeclProcessor.additionalDeclsFile=/path/to/file
         * </code>
         * 
         * {@see https://jira.blazegraph.com/browse/BLZG-1773}
         *      
         */
        public static final String ADDITIONAL_DECLS_FILE = PrefixDeclProcessor.class.getName()
                + ".additionalDeclsFile"; 
        
    }
    
    static {
        defaultDecls.put("rdf", RDF.NAMESPACE);
        defaultDecls.put("rdfs", RDFS.NAMESPACE);
        defaultDecls.put("sesame", SESAME.NAMESPACE);
        defaultDecls.put("owl", OWL.NAMESPACE);
        defaultDecls.put("xsd", XMLSchema.NAMESPACE);
        defaultDecls.put("fn", FN.NAMESPACE);
        defaultDecls.put("foaf", FOAF.NAMESPACE);
        defaultDecls.put("dc", DC.NAMESPACE);
        defaultDecls.put("hint", QueryHints.NAMESPACE);
        defaultDecls.put("bd", BD.NAMESPACE);
        defaultDecls.put("bds", BDS.NAMESPACE);
        //Add any additional decls passed via property
        //SEE BLZG-1773
    	processAdditionalDecls();
    }
    
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

        // insert some default prefixes (if not explicitly defined in the query)
//      insertDefaultPrefix(prefixMap, "rdf", RDF.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "rdfs", RDFS.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "sesame", SESAME.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "owl", OWL.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "xsd", XMLSchema.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "fn", FN.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "hint", QueryHints.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "bd", BD.NAMESPACE);
//      insertDefaultPrefix(prefixMap, "bds", BDS.NAMESPACE);
        for (Map.Entry<String, String> e : defaultDecls.entrySet()) {
            insertDefaultPrefix(prefixMap, e.getKey(), e.getValue());
        }

        ASTUnparsedQuadDataBlock dataBlock = null;
        if (qc.getOperation() instanceof ASTInsertData) {
            ASTInsertData insertData = (ASTInsertData)qc.getOperation();
            dataBlock = insertData.jjtGetChild(ASTUnparsedQuadDataBlock.class);

        }
        else if (qc.getOperation() instanceof ASTDeleteData) {
            ASTDeleteData deleteData = (ASTDeleteData)qc.getOperation();
            dataBlock = deleteData.jjtGetChild(ASTUnparsedQuadDataBlock.class);
        }

        if (dataBlock != null) {
            String prefixes = createPrefixesInSPARQLFormat(prefixMap);
            // TODO optimize string concat?
            dataBlock.setDataBlock(prefixes + dataBlock.getDataBlock());
        }
        else {
        QNameProcessor visitor = new QNameProcessor(prefixMap);
        try {
            qc.jjtAccept(visitor, null);
        }
        catch (VisitorException e) {
            throw new MalformedQueryException(e);
        }
        }

        return prefixMap;
    }

    private static void insertDefaultPrefix(Map<String, String> prefixMap, String prefix, String namespace) {
        if (!prefixMap.containsKey(prefix) && !prefixMap.containsValue(namespace)) {
            prefixMap.put(prefix, namespace);
        }
    }

    private static String createPrefixesInSPARQLFormat(Map<String, String> prefixMap) {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> entry : prefixMap.entrySet()) {
            sb.append("PREFIX");
            final String prefix = entry.getKey();
            if (prefix != null) {
                sb.append(" " + prefix);
            }
            sb.append(":");
            sb.append(" <" + entry.getValue() + ">");
        }
        return sb.toString();
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
            String qname = qnameNode.getValue();

            int colonIdx = qname.indexOf(':');
            assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;

            String prefix = qname.substring(0, colonIdx);
            String localName = qname.substring(colonIdx + 1);

            String namespace = prefixMap.get(prefix);
            if (namespace == null) {
                throw new VisitorException("QName '" + qname + "' uses an undefined prefix");
            }

            localName = processEscapesAndHex(localName);

            // Replace the qname node with a new IRI node in the parent node
            ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
            iriNode.setValue(namespace + localName);
            qnameNode.jjtReplaceWith(iriNode);

            return null;
        }
    
        private String processEscapesAndHex(String localName) {
            
            // first process hex-encoded chars.
            StringBuffer unencoded = new StringBuffer();
            Pattern hexPattern = Pattern.compile("([^\\\\]|^)(%[A-F\\d][A-F\\d])", Pattern.CASE_INSENSITIVE);
            Matcher m = hexPattern.matcher(localName);
                boolean result = m.find();
                while (result) {
                // we match the previous char because we need to be sure we are not
                // processing an escaped % char rather than
                    // an actual hex encoding, for example: 'foo\%bar'.
                String previousChar = m.group(1);
                String encoded = m.group(2);
    
                int codePoint = Integer.parseInt(encoded.substring(1), 16);
                String decoded = String.valueOf(Character.toChars(codePoint));
                    
                    m.appendReplacement(unencoded, previousChar + decoded);
                    result = m.find();
                }
                m.appendTail(unencoded);

            // then process escaped special chars.
            StringBuffer unescaped = new StringBuffer();
            Pattern escapedCharPattern = Pattern.compile("\\\\[_~\\.\\-!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:\\/\\?#\\@\\%]");
            m = escapedCharPattern.matcher(unencoded.toString());
            result = m.find();
                while (result) {
                String escaped = m.group();
                    m.appendReplacement(unescaped, escaped.substring(1));
                    result = m.find();
                }
                m.appendTail(unescaped);
            
            return unescaped.toString();
        }

        @Override
        public Object visit(ASTServiceGraphPattern node, Object data)
            throws VisitorException
        {
            node.setPrefixDeclarations(prefixMap);
            return super.visit(node, data);
        }

        /**
         * Provide silent declaration for some well known namspaces.
         */
        @SuppressWarnings("unused")
		private String checkForWellKnownNamespacePrefix(final String prefix) {
            final String namespace;
            if (prefix.equals("bd")) {
                prefixMap.put("bd", namespace = BD.NAMESPACE);
            } else if (prefix.equals("bds")) {
                prefixMap.put("bds", namespace = BDS.NAMESPACE);
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
            } else if (prefix.equals("gas")) {
                prefixMap.put("gas", namespace = GASService.Options.NAMESPACE);
            } else {
                // Unknown
                namespace = null;
            }
            return namespace;
        }

    }
    
    /**
     * 
     * Static helper method to process the {@link PrefixDeclProcessor.Options.ADDTIONAL_DECLS_FILE}
     * property if present and add the decls.  It ignores IO errors, etc. and
     * provides a warning. 
     * 
     * {@see BLZG-1773}
     * 
     * @author beebs
     * 
     */
	public static void processAdditionalDecls() {

		final String declsFile = System
				.getProperty(Options.ADDITIONAL_DECLS_FILE);

		if (declsFile != null && !declsFile.equals("")) {

			final File f = new File(declsFile);

			if (!f.exists()) {
				log.warn(declsFile
						+ " passed by -D"
						+ Options.ADDITIONAL_DECLS_FILE
						+ " does not exist.  Ignoring.  Additional Decls will not be set.");
				return;
			}

			if (!f.canRead()) {
				log.warn(declsFile
						+ " passed by -D"
						+ Options.ADDITIONAL_DECLS_FILE
						+ " is not readable.  Ignoring.  Additional Decls will not be set.");
				return;
			}

			try {
				FileReader r = new FileReader(f);
				int ch = r.read();
				final StringBuffer sb = new StringBuffer();
				boolean done = false;
				while (!done) {
					if ((char) ch == '\n' || ch == -1) { // End of line or last line
						
						done = ch == -1;
						
						final String s = sb.toString();
						
						if(sb.length() == 0) {
							//End of file with no input
							break;
						}
					
						sb.delete(0, sb.length());
						// 0 1 2
						// PREFIX wdref: <http://www.wikidata.org/reference/>
						final String[] decls = s.split(" ");

						if (decls.length != 3) {
							log.warn(declsFile + " line:  " + s
									+ " is not valid.  Ignoring.");
							log.warn("Expecting lines formatted as: \"PREFIX wdref: <http://www.wikidata.org/reference/>\"");
							ch = r.read();
							continue; // process the next line
						}

						if (!decls[1].endsWith(":")) {
							log.warn(declsFile + " line:  " + s
									+ " is not valid. Ignoring.");
							log.warn(decls[1] + " does not end with :");
							ch = r.read();
							continue; // process the next line
						}

						// String :
						final String prefix = decls[1].substring(0,
								decls[1].length() - 1);

						if (!decls[2].endsWith(">")
								&& !decls[2].startsWith("<")) {
							log.warn(declsFile + " line:  " + s
									+ " is not valid.  Ignoring.");
							log.warn(decls[2]
									+ " does not start and end with < and >, respectively.");
							ch = r.read();
							continue; // process the next line
						}

						// String < and >
						final String uri = decls[2].substring(1,
								decls[2].length() - 1);
						
						log.warn("Configured prefix: PREFIX " + prefix + ": " + uri);
						
						defaultDecls.put(prefix, uri);

					} else {
						sb.append((char)ch);
					}
					ch = r.read();

				}
				
				r.close();

			} catch (IOException e) {
				log.warn(e.toString()
						+ "\n while processing "
						+ declsFile
						+ " passed by -D"
						+ Options.ADDITIONAL_DECLS_FILE
						+ "does not exist.  Ignoring.  Additional Decls will not be set.");
			}
		}

	}

}
