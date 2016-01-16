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
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Mar 27, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import info.aduna.lang.FileFormat;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;

/**
 * Utility class for generating accept heades modeled on
 * {@link RDFFormat#getAcceptParams(Iterable, boolean, RDFFormat)}, but extended
 * to handle {@link TupleQueryResultFormat} using the same base quality value.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Add a parameter for SID support or just specify as the
 *          preferredFormat when the database is using SIDs (or for any bigdata
 *          database if we support SIDs in all modes).
 * 
 *          TODO It would be nice if the formats indicated their compactness and
 *          unicode support capability. Given that other things are equal, we
 *          would prefer a format which was compact and supported unicode.
 */
public class AcceptHeaderFactory {

    private static final int defaultQValue = 10;

    /**
     * 
     * @param rdfFormats
     * @param requireContext
     * @param preferredFormat
     * @return
     */
    public static List<String> getAcceptParams(
            final Iterable<RDFFormat> rdfFormats, //
            final boolean requireContext,//
            final RDFFormat preferredFormat//
            ) {
        
        final List<String> acceptParams = new LinkedList<String>();

        for (RDFFormat format : rdfFormats) {
            // Determine a q-value that reflects the necessity of context
            // support and the user specified preference
            int qValue = defaultQValue;

            if (requireContext && !format.supportsContexts()) {
                // Prefer context-supporting formats over pure triple-formats
                qValue -= 5;
            }

            if (preferredFormat != null && !preferredFormat.equals(format)) {
                // Prefer specified format over other formats
                qValue -= 2;
            }

            if (!format.supportsNamespaces()) {
                // We like reusing namespace prefixes
                qValue -= 1;
            }

            for (String mimeType : format.getMIMETypes()) {

                // Default is the bare mime type.
                String acceptParam = mimeType;

                if (qValue < 10) {
                
                    // Annotate with the factional quality score.
                    acceptParam += ";q=0." + qValue;
                    
                }

                acceptParams.add(acceptParam);

            }
            
        }

        return acceptParams;

    }

    /**
     * Return a set of accept header values annotated with quality scores.
     * 
     * @param formats
     *            The set of formats which can be accepted.
     * @param preferredFormat
     *            The preferred format (optional).
     *            
     * @return The list of accept header values with quality scores.
     */
    public static <T extends FileFormat> List<String> getAcceptParams(
            final Iterable<T> formats,
            final T preferredFormat) {
        
        final List<String> acceptParams = new LinkedList<String>();

        for (T format : formats) {
            
            // Determine a q-value that reflects the user specified preference
            int qValue = defaultQValue;

            if (preferredFormat != null && !preferredFormat.equals(format)) {
                // Prefer specified format over other formats
                qValue -= 2;
            }

            for (String mimeType : format.getMIMETypes()) {

                // Default is the bare mime type
                String acceptParam = mimeType;

                if (qValue < 10) {
                
                    // Annotate with the factional quality score.
                    acceptParam += ";q=0." + qValue;
                    
                }

                acceptParams.add(acceptParam);
                
            }

        }

        return acceptParams;
    
    }
    
    /**
     * Return an accept header which establishes a preference pattern for graph
     * data. The accept header will not include any formats for which we can not
     * discover a parser.
     */
    public static String getDefaultGraphAcceptHeader(final boolean requireContext) {
       
        // Copy into a Set.
        final Set<RDFFormat> values = new LinkedHashSet<RDFFormat>(
                RDFFormat.values());

        final RDFParserRegistry registry = RDFParserRegistry.getInstance();

        final Iterator<RDFFormat> itr = values.iterator();

        while (itr.hasNext()) {

            final RDFFormat format = itr.next();

            if (registry.get(format) == null) {

                /*
                 * Remove any format for which there is no registered parser.
                 */

                itr.remove();

            }

        }
        final List<String> list1 = AcceptHeaderFactory.getAcceptParams(values,
                requireContext, RDFFormat.BINARY);

        return toString(list1);
        
    }

    /**
     * Return an accept header which establishes a preference pattern for
     * solution set data. The accept header will not include any formats for
     * which we can not discover a parser.
     * <p>
     * Note: You CAN NOT just combine the accept headers for boolean results and
     * solution sets together because the boolean format overlaps the solution
     * set format (they are both [application/sparql-results+xml] so putting
     * them together blurs the quality annotations.
     */
    public static String getDefaultSolutionsAcceptHeader() {
       
        // Copy into a Set.
        final Set<TupleQueryResultFormat> values = new LinkedHashSet<TupleQueryResultFormat>(
                TupleQueryResultFormat.values());
        
        final TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry
                .getInstance();
        
        final Iterator<TupleQueryResultFormat> itr = values.iterator();

        while (itr.hasNext()) {

            final TupleQueryResultFormat format = itr.next();

            if (registry.get(format) == null) {

                /*
                 * Remove any format for which there is no registered parser.
                 * 
                 * Note: For example, the JSON parser does not exist for openrdf
                 * 2.6.3.
                 */
                
                itr.remove();

            }

        }
        
        final List<String> list2 = AcceptHeaderFactory.getAcceptParams(values,
                TupleQueryResultFormat.BINARY);

        return toString(list2);
        
    }

    /**
     * Return an accept header which establishes a preference pattern for
     * boolean data.  The accept header will not include any formats for
     * which we can not discover a parser.
     */
    public static String getDefaultBooleanAcceptHeader() {

        // Copy into a Set.
        final Set<BooleanQueryResultFormat> values = new LinkedHashSet<BooleanQueryResultFormat>(
                BooleanQueryResultFormat.values());
        
        final BooleanQueryResultParserRegistry registry = BooleanQueryResultParserRegistry
                .getInstance();
        
        final Iterator<BooleanQueryResultFormat> itr = values.iterator();

        while (itr.hasNext()) {

            final BooleanQueryResultFormat format = itr.next();

            if (registry.get(format) == null) {

                /*
                 * Remove any format for which there is no registered parser.
                 */
                
                itr.remove();

            }

        }
        
        final List<String> list3 = AcceptHeaderFactory.getAcceptParams(
                values,
                BooleanQueryResultFormat.SPARQL);

        return toString(list3);
        
    }

    /**
     * Convert into a comma delimited list.
     * 
     * @param values
     *            The values.
     *            
     * @return The comma delimited list.
     */
    private static String toString(final Iterable<String> values) {

        final StringBuilder sb = new StringBuilder();

        boolean first = true;

        for (String s : values) {

            if (first) {

                first = false;

            } else {

                sb.append(",");

            }

            sb.append(s);
        }

        final String s = sb.toString();

        return s;

    }

}
