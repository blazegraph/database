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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.Writer;

import com.bigdata.util.HTMLUtility;

/**
 * Utility Java class for outputting XML.
 * <p>
 * The motivation is to provide a similar interface to a document builder but
 * instead of building an in-memory model, to directly stream the output.
 * <p>
 * Example usage:
 * <pre>
 * 	<xml>
 *    <div attr="attr1">Content</div>
 *    <div attr="attr1"/>
 *    <div>Content</div>
 *    <div>
 *    	<div>Content</div>
 *    </div>
 *  </xml>
 *  
 *  XMLBuilder.Node closed = new XMLBuilder(false,writer)
 *  	.root("xml")
 *  		.node("div")
 *  			.attr("attr", "attr1")
 *  			.text("Content")
 *  			.close()
 *  		.node("div", "Content")
 *  		.node("div")
 *  			.node("div", "Content")
 *  			.close()
 *  		.close();
 *  
 *  // The close on the root node will return null
 *  
 *  assert(closed == null);
 * </pre>
 * 
 * @author Martyn Cutcher
 */
public class XMLBuilder {
    
	private final boolean xml;
	private final Writer m_writer;
	
//	private boolean m_pp = false;
	
//	public XMLBuilder() throws IOException {
//
//		this(true, (OutputStream) null);
//		
//	}
//	
//	public XMLBuilder(boolean xml) throws IOException {
//
//		this(xml, (OutputStream) null);
//		
//	}
//	
//	public XMLBuilder(final boolean xml, final OutputStream outstr)
//			throws IOException {
//
//		this(xml, null/* encoding */, outstr);
//		
//	}
//	
//	public XMLBuilder(final boolean xml, final String encoding)
//			throws IOException {
//
//	    this(xml, encoding, (OutputStream) null);
//	    
//	}

	/**
	 * Return the backing {@link Writer}.
	 */
	public Writer getWriter() {
	    
	    return m_writer;
	    
	}
	
	public XMLBuilder(final Writer w) throws IOException {

		this(true/* xml */, null/* encoding */, w/* writer */);
		
	}
	
	public XMLBuilder(final boolean xml, final String encoding,
			final Writer w) throws IOException {

		if(w == null)
			throw new IllegalArgumentException();
		
		this.xml = xml;
		
		this.m_writer  = w;
		
		if (xml) {
			if (encoding != null) {
				m_writer.write("<?xml version=\"1.0\" encoding=\"" + encoding + "\"?>");
			} else {
				m_writer.write("<?xml version=\"1.0\"?>");
			}
		} else {
			/*
			 * Note: The optional encoding should also be included in a meta tag
			 * for an HTML document.
			 */
			m_writer.write("<!DOCTYPE HTML PUBLIC");
			m_writer.write(" \"-//W3C//DTD HTML 4.01 Transitional//EN\"");
			m_writer.write(" \"http://www.w3.org/TR/html4/loose.dtd\">");
		}
		
	}
	
//	public void prettyPrint(boolean pp) {
//		m_pp = pp;
//	}
	
//	private void initWriter(OutputStream outstr) {
//	}

	// Note: This method assumed that m_writer was a StringWriter!!!
//	public String toString() {
//		return m_writer.toString();
//	}
	
	public Node root(String name) throws IOException {
		return new Node(name, null);
	}
	
	public Node root(String name, String nodeText) throws IOException {
		Node root = new Node(name, null);
		root.text(nodeText);
		
		return root.close();
	}
	
	public void closeAll(Node node) throws IOException {
		while (node != null) {
			node = node.close();
		}
	}
	
	public class Node {

	    private boolean m_open = true;
		private String m_tag;
		private Node m_parent;
		private int m_attrs = 0;
		private int m_text = 0;
		private int m_nodes = 0;
		
		Node(final String name, final Node parent) throws IOException {
			m_tag = name;
			m_parent = parent;
			
			m_writer.write("<" + m_tag);
        }

		public String toString() {

		    return "<"
                    + m_tag
                    + "...>"
                    + (m_parent == null ? "" : "(parent=" + m_parent.m_tag
                            + ")");
		
		}
		
        /**
         * Return the {@link XMLBuilder} to which this {@link Node} belongs.
         */
        public XMLBuilder getBuilder() {

            return XMLBuilder.this;
            
        }
		
        /**
         * Add an attribute value to an open element head.
         * 
         * @param attr
         *            The name of the attribute.
         * @param value
         *            The value of the attribute, which will be automatically
         *            encoded.
         * 
         * @return This {@link Node}
         * 
         * @throws IOException
         */
        public Node attr(final String attr, final Object value)
                throws IOException {
            
			m_writer.write(" " + attr + "=\"" + attrib(value.toString()) + "\"");
			m_attrs++;
			
			return this;
		}
		
        /**
         * Write out the text.
         * <P>
         * The head for the current element is first closed if it was still
         * open. Then the text is then encoded and written out.
         * <p>
         * This does not generate the closing tag for the node. More text or
         * embedded nodes can still be output.
         * 
         * @param text
         *            The text, which will be automatically encoded in a manner
         *            suitable for a CDATA section.
         * 
         * @return This {@link Node}
         * 
         * @throws IOException
         */
		public Node text(final String text) throws IOException {
			
		    closeHead();
			
			m_writer.write(cdata(text));
			m_text++;
			
			return this;
		}
		
        /**
         * Write out the text without encoding for (X)HTML.
         * <P>
         * The head for the current element is first closed if it was still
         * open. Then the text is NOT encoded before written out.
         * <p>
         * This does not generate the closing tag for the node. More text or
         * embedded nodes can still be output.
         * 
         * @param text
         *            The text, which will be automatically encoded in a manner
         *            suitable for a CDATA section.
         * 
         * @return This {@link Node}
         * 
         * @throws IOException
         */
		public Node textNoEncode(final String text) throws IOException {
            
            closeHead();
            
            m_writer.write(text);
            m_text++;
            
            return this;
        }
        
        /**
         * Create and return a new {@link Node}. The head of the {@link Node}
         * will be open. Attributes may be specified until the closing tag is
         * generated by {@link Node#close()} or until the head of the node is
         * closed by {@link Node#text(String)}.
         * 
         * @param tag
         *            The tag for that {@link Node}.
         * 
         * @return The new {@link Node}.
         * 
         * @throws IOException
         */
		public Node node(final String tag) throws IOException {
			
		    closeHead();
			
			m_nodes++;
			
			return new Node(tag, this);
			
		}

        /**
         * Generate an open element for the tag, output the indicated text, and
         * then output the closing element for that tag.
         * 
         * @param tag
         *            The tag (element name).
         * @param text
         *            The text, which will be automatically encoded as
         *            appropriate for a CDATA section.
         * 
         * @return This {@link Node}.
         * 
         * @throws IOException
         */
        public Node node(final String tag, final String text)
                throws IOException {

            closeHead();

            m_nodes++;

            final Node tmp = new Node(tag, this);

            tmp.text(text);
			
			final Node ret = tmp.close();
			
			assert ret == this;
			
			return ret;
		}

        /**
         * Close the open element.
         * 
         * @return The parent element.
         * 
         * @throws IOException
         */
		public Node close() throws IOException {
		
		    if(xml) {
		        // Always do an END element for XML.
		        return close(false/*simpleEnd*/);
		    }
		    return close(isSimpleEndTagForHTML());
		    
		}

        /**
         * Return <code>true</code> if this is an HTML element which can be
         * closed without an XML style end tag.
         * 
         * TODO This does not cover all cases by a long shot. It just covers the
         * ones that we are currently using.
         * 
         * TODO This should be done with a hash map for faster lookup once there
         * are more tags that we recognize in this method.
         */
        private boolean isSimpleEndTagForHTML() {

            if ("form".equalsIgnoreCase(m_tag)) {
            
                return false;
                
            }
            
            return true;
            
        }
		
		/**
		 * Close the open element.
		 * 
		 * @param simpleEnd
		 *            When <code>true</code> an open tag without a body will be
		 *            closed by a single &gt; symbol rather than the XML style
		 *            &#47;&gt;.
		 * 
		 * @return The parent element.
		 * @throws IOException
		 */
		public Node close(final boolean simpleEnd) throws IOException {
			assert(m_open);
			
			if (emptyBody()) {
				if(simpleEnd) {
					m_writer.write(">");
				} else {
					m_writer.write("/>");
				}
			} else {
				m_writer.write("</" + m_tag + "\n>");
			}
			
			m_open = false;
			
			// If root node then flush the writer
			if (m_parent == null) {
				m_writer.flush();
			}
			
			return m_parent;
		}
		
		private void closeHead() throws IOException {
			if (emptyBody()) {
				m_writer.write(">");
			}
		}
		
		private boolean emptyBody() {
			return m_nodes == 0 && m_text == 0;
		}
		
	}

    /**
     * Encode a string for including in a CDATA section.
     * 
     * @param s
     *            The string.
     * 
     * @return The encoded string.
     */
    static private String cdata(final String s) {

        if (s == null)
            throw new IllegalArgumentException();
        
        return HTMLUtility.escapeForXHTML(s);
        
    }
    
    /**
     * Encoding a string for including in an (X)HTML attribute value.
     * 
     * @param s
     *            The string.
     *            
     * @return
     */
    static private String attrib(final String s) {
        
        return HTMLUtility.escapeForXHTML(s);
        
    }
    
}
