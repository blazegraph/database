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
/*
 * Created on Apr 18, 2008
 */

package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;

public enum DoctypeEnum {

    html_4_01_strict("-//W3C//DTD HTML 4.01//EN",
            "http://www.w3.org/TR/html4/strict.dtd", false),

    html_4_01_transitional("-//W3C//DTD HTML 4.01 Transitional//EN",
            "http://www.w3.org/TR/html4/loose.dtd", false),

    xhtml_1_0_strict("-//W3C//DTD XHTML 1.0 Strict//EN",
            "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd", true);

    private DoctypeEnum(String publicId, String systemId, boolean xml) {
        
        this.publicId = publicId;
        
        this.systemId = systemId;
        
        this.xml = xml;
        
    }
    
    private final String publicId;
    private final String systemId;
    private final boolean xml;
    
    public String publicId() {

        return publicId;
        
    }

    public String systemId() {
        
        return systemId;
        
    }

    public boolean isXML() {
        
        return xml;
        
    }

    /**
     * Writes the W3C "valid" icon into the page for the {@link DoctypeEnum}.
     * 
     * @param w
     * 
     * @throws IOException
     */
    public void writeValid(Writer w) throws IOException {
       
        switch(this) {
        case html_4_01_strict:
            
            writeValidHTML401Strict(w);
            
            break;
            
        case html_4_01_transitional:
            
            writeValidHTML401Transitional(w);
            
            break;
            
        case xhtml_1_0_strict:
            
            writeValidXHTML(w);
            
            break;
            
        default:
            
            throw new UnsupportedOperationException(this.toString());
        
        }
        
    }
    
    private void writeValidXHTML(Writer w) throws IOException {
        
        w.write("<p>");
        w.write("<a href=\"http://validator.w3.org/check?uri=referer\">");
        w.write("<img"); // Note: no "border" attribute - use CSS.
        w.write(" src=\"http://www.w3.org/Icons/valid-xhtml10\"");
        w.write(" alt=\"Valid XHTML 1.0 Strict\" height=\"31\" width=\"88\"/>");
        w.write("</a>");
        w.write("</p>");

    }
    
    private void writeValidHTML401Strict(Writer w) throws IOException {
    
        w.write("<p>");
        w.write("<a href=\"http://validator.w3.org/check?uri=referer\">");
        w.write("<img"); // Note: no "border" attribute - use CSS.
        w.write(" src=\"http://www.w3.org/Icons/valid-html401\"");
        w.write(" alt=\"Valid HTML 4.01 Transitional\" height=\"31\" width=\"88\">");
        w.write("</a>");
        w.write("</p>");

    }

    private void writeValidHTML401Transitional(Writer w) throws IOException {
        
        w.write("<p>");
        w.write("<a href=\"http://validator.w3.org/check?uri=referer\">");
        w.write("<img border=\"0\"");
        w.write(" src=\"http://www.w3.org/Icons/valid-html401\"");
        w.write(" alt=\"Valid HTML 4.01 Transitional\" height=\"31\" width=\"88\">");
        w.write("</a>");
        w.write("</p>");

    }
 
}