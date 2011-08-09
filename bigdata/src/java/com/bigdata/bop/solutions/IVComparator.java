/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/* Portions Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 8, 2011
 */

package com.bigdata.bop.solutions;

import info.aduna.lang.ObjectUtil;

import java.util.Comparator;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;

/**
 * A comparator that compares {@link IV}s according the SPARQL value ordering as
 * specified in <A
 * href="http://www.w3.org/TR/rdf-sparql-query/#modOrderBy">SPARQL Query
 * Language for RDF</a>. This implementation is based on the openrdf
 * {@link ValueComparator} but has been modified to work with {@link IV}s.
 * 
 * @author james
 * @author Arjohn Kampman
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class IVComparator implements Comparator<IV> {

    public int compare(final IV o1, final IV o2) {

        // check equality
        if (ObjectUtil.nullEquals(o1, o2)) {
            return 0;
        }

        // 1. (Lowest) no value assigned to the variable
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }

        // 2. Blank nodes
        final boolean b1 = o1.isBNode();
        final boolean b2 = o2.isBNode();
        if (b1 && b2) {
            return 0;
        }
        if (b1) {
            return -1;
        }
        if (b2) {
            return 1;
        }

        // 3. IRIs
        final boolean u1 = o1.isURI();
        final boolean u2 = o2.isURI();
        if (u1 && u2) {
            return compareURIs(o1, o2);
        }
        if (u1) {
            return -1;
        }
        if (u2) {
            return 1;
        }

        // 4. RDF literals
        return compareLiterals((IV<BigdataLiteral, ?>) o1,
                (IV<BigdataLiteral, ?>) o2);
    
    }

    static private int compareURIs(final IV<BigdataURI, URI> left,
            final IV<BigdataURI, URI> right) {

        // TODO Support inline URIs here. E.g.,
        // if (o1.isInline() && o2.isInline())
        // return o1.compareTo(o2);

        return compareURIs((URI) left.getValue(), (URI) right.getValue());

    }

    static private int compareURIs(final URI leftURI, final URI rightURI) {

        return leftURI.toString().compareTo(rightURI.toString());
        
    }

    /**
     * Compare two {@link IV}s which model literals.
     * <p>
     * Note: We can optimize many cases when comparing literals based on
     * recognition of inline IVs having the same datatype.
     */
    static private int compareLiterals(final IV<BigdataLiteral, ?> left,
            final IV<BigdataLiteral, ?> right) {

        /*
         * First, try to handle comparison of two inline IVs or one inline IV
         * with one materialized Literal. If we can not manage that then we will
         * be forced to compare two materialized literals.
         */
        if (IVUtility.canNumericalCompare(left)) {
            if (IVUtility.canNumericalCompare(right)) {
                return IVUtility.numericalCompare(left, right);
            }
            final Literal rightLit = right.getValue();
            if (rightLit == null)
                throw new NotMaterializedException();
            if (rightLit.getDatatype() != null) {
                // Compare inline IV with materialized Literal.
                return IVUtility.numericalCompare(left, rightLit);
            }
        } else if (IVUtility.canNumericalCompare(right)) {
            final Literal leftLit = left.getValue();
            if (leftLit == null)
                throw new NotMaterializedException();
            if (leftLit.getDatatype() != null) {
                // Compare materialized Literal with inline IV.
                return -IVUtility.numericalCompare(right, leftLit);
            }
        }
        return compareMaterializedLiterals(left, right);
    }
    
    /**
     * Handle case of non-inline literals.
     */
    static private int compareMaterializedLiterals(
            final IV<BigdataLiteral, ?> left, final IV<BigdataLiteral, ?> right) {

        final BigdataLiteral val1 = left.getValue();
        final BigdataLiteral val2 = right.getValue();

        if (val1 == null || val2 == null)
            throw new NotMaterializedException();

        try {

            return compareLiterals(val1, val2);

        } catch (Exception ex) {

            throw new SparqlTypeErrorException();

        }
    }
    
    static private int compareLiterals(final Literal leftLit,
            final Literal rightLit) {
        
        // Additional constraint for ORDER BY: "A plain literal is lower
        // than an RDF literal with type xsd:string of the same lexical
        // form."

        if (!QueryEvaluationUtil.isStringLiteral(leftLit)
                || !QueryEvaluationUtil.isStringLiteral(rightLit)) {

            try {

                final boolean isSmaller = QueryEvaluationUtil.compareLiterals(
                        leftLit, rightLit, CompareOp.LT);

                if (isSmaller) {
                    return -1;
                }
                else {
                    return 1;
                }
            }
            catch (ValueExprEvaluationException e) {
                // literals cannot be compared using the '<' operator, continue
                // below
            }
        }

        int result = 0;

        // Sort by datatype first, plain literals come before datatyped literals
        final URI leftDatatype = leftLit.getDatatype();
        final URI rightDatatype = rightLit.getDatatype();

        if (leftDatatype != null) {
            if (rightDatatype != null) {
                // Both literals have datatypes
                result = compareDatatypes(leftDatatype, rightDatatype);
            }
            else {
                result = 1;
            }
        }
        else if (rightDatatype != null) {
            result = -1;
        }

        if (result == 0) {
            // datatypes are equal or both literals are untyped; sort by language
            // tags, simple literals come before literals with language tags
            final String leftLanguage = leftLit.getLanguage();
            final String rightLanguage = rightLit.getLanguage();

            if (leftLanguage != null) {
                if (rightLanguage != null) {
                    result = leftLanguage.compareTo(rightLanguage);
                }
                else {
                    result = 1;
                }
            }
            else if (rightLanguage != null) {
                result = -1;
            }
        }

        if (result == 0) {
            // Literals are equal as far as their datatypes and language tags are
            // concerned, compare their labels
            result = leftLit.getLabel().compareTo(rightLit.getLabel());
        }

        return result;
    }

    /**
     * Compares two literal datatypes and indicates if one should be ordered
     * after the other. This algorithm ensures that compatible ordered datatypes
     * (numeric and date/time) are grouped together so that
     * {@link QueryEvaluationUtil#compareLiterals(Literal, Literal, CompareOp)}
     * is used in consecutive ordering steps.
     */
    static private int compareDatatypes(final URI leftDatatype,
            final URI rightDatatype) {
        
        if (XMLDatatypeUtil.isNumericDatatype(leftDatatype)) {
            if (XMLDatatypeUtil.isNumericDatatype(rightDatatype)) {
                // both are numeric datatypes
                return compareURIs(leftDatatype, rightDatatype);
            }
            else {
                return -1;
            }
        }
        else if (XMLDatatypeUtil.isNumericDatatype(rightDatatype)) {
            return 1;
        }
        else if (XMLDatatypeUtil.isCalendarDatatype(leftDatatype)) {
            if (XMLDatatypeUtil.isCalendarDatatype(rightDatatype)) {
                // both are calendar datatypes
                return compareURIs(leftDatatype, rightDatatype);
            }
            else {
                return -1;
            }
        }
        else if (XMLDatatypeUtil.isCalendarDatatype(rightDatatype)) {
            return 1;
        }
        else {
            // incompatible or unordered datatypes
            return compareURIs(leftDatatype, rightDatatype);
        }
    }

}