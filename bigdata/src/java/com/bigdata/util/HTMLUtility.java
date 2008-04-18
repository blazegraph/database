/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
package com.bigdata.util;

/**
 * Collection of some utility methods for HTML.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HTMLUtility {

    /**
     * 
     */
    public HTMLUtility() {
        super();
    }

    /**
     * <p>
     * Sometimes you want to escape something without using a DOM instance. This
     * method escapes a String value so that it may be written as the value of
     * an XML attribute in a manner that is also compatible with HTML. Note that
     * the best solution is to use a DOM instance, which will automatically
     * escape attribute values and PCDATA as they are inserted into the DOM
     * instance.
     * </p>
     * <p>
     * The following notes are excerpted from the HTML and XML specifications
     * <ul>
     * <li>HTML: By default, SGML requires that all attribute values be
     * delimited using either double quotation marks (ASCII decimal 34) or
     * single quotation marks (ASCII decimal 39). Single quote marks can be
     * included within the attribute value when the value is delimited by double
     * quote marks, and vice versa. Authors may also use numeric character
     * references to represent double quotes (&#34;) and single quotes (&#39;).
     * For double quotes authors can also use the character entity reference
     * &quot;.<br>
     * In certain cases, authors may specify the value of an attribute without
     * any quotation marks. The attribute value may only contain letters (a-z
     * and A-Z), digits (0-9), hyphens (ASCII decimal 45), periods (ASCII
     * decimal 46), underscores (ASCII decimal 95), and colons (ASCII decimal
     * 58). We recommend using quotation marks even when it is possible to
     * eliminate them.</li>
     * <li>XML: The ampersand character (&) and the left angle bracket (<) may
     * appear in their literal form only when used as markup delimiters, or
     * within a comment, a processing instruction, or a CDATA section. If they
     * are needed elsewhere, they must be escaped using either numeric character
     * references or the strings "&amp;" and "&lt;" respectively. The right
     * angle bracket (>) may be represented using the string "&gt;", and must,
     * for compatibility, be escaped using "&gt;" or a character reference when
     * it appears in the string "]]>" in content, when that string is not
     * marking the end of a CDATA section.</li>
     * </ul>
     * </p>
     */

    public static String escapeForXHTML(String s) {

        if( s == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        int len = s.length();

        if (len == 0)
            return s;

        StringBuffer sb = new StringBuffer(len + 20);

        for (int i = 0; i < len; i++) {

            char ch = s.charAt(i);

            switch (ch) {

            case '"':
                sb.append("&#34;");
                break;

            case '\'':
                sb.append("&#39;");
                break;

            case '&':
                sb.append("&amp;");
                break;

            case '<':
                sb.append("&lt;");
                break;

            case '>':
                sb.append("&gt;");
                break;

            default:
                sb.append(ch);
                break;

            }

        }

	return sb.toString();

    }


//    /**
//     * Same as escapeForXHTML but respects the encoding
//     * parameter.
//     */	
//    public static String escapeForXHTML(String s, String enc) 
//	throws java.io.UnsupportedEncodingException
//    {
//
//	String retval = escapeForXHTML(s);
//	
//	return new String(retval.getBytes(enc), enc);
//    }

    public static String escapeForXMLName(String s) {

        if( s == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        int len = s.length();

        if (len == 0)
            return s;

        StringBuffer sb = new StringBuffer(len + 20);

	char ch = s.charAt(0);

	if(Character.isDigit(ch))
	{
		sb.append("_num_");
	}

        for (int i = 0; i < len; i++) {

            ch = s.charAt(i);

            switch (ch) {

            case '"':
                sb.append("_quote_");
                break;

            case '\'':
                sb.append("_apos_");
                break;

            case '&':
                sb.append("_amp_");
                break;

            case '<':
                sb.append("_lt_");
                break;

            case '>':
                sb.append("_gt_");
                break;

            case '$':
                sb.append("_dollar_");
                break;

            case ':':
                sb.append("_colon_");
                break;

            case '~':
                sb.append("_tilda_");
                break;

            case '(':
                sb.append("_lparen_");
                break;

            case ')':
                sb.append("_rparen_");
                break;

            case ',':
                sb.append("_comma_");
                break;

            case '=':
                sb.append("_eq_");
                break;

            case '!':
                sb.append("_bang_");
                break;

            case '?':
                sb.append("_quest_");
                break;

            case '/':
                sb.append("_fw_slash_");
                break;

            case '\\':
                sb.append("_bk_slash_");
                break;

            case ';':
                sb.append("_semicolon_");
                break;

            case '.':
                sb.append("_period_");
                break;

            case '`':
                sb.append("_tic_");
                break;

            default:
                sb.append(ch);
                break;

            }

        }

        return sb.toString();

    }

}
