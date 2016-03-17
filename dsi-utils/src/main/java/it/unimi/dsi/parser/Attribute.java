package it.unimi.dsi.parser;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.lang.MutableString;

/** An SGML attribute. */
public final class Attribute {

    /** The name of this attribute. */
	public final CharSequence name;
	
	/** Creates a new attribute with given name.
	 *
	 * @param name the name of the new attribute.
	 */
	public Attribute( final CharSequence name ) {
		this.name = new MutableString( name );
	}

	/** Returns the name of this attribute.
	 * @return the name of this attribute.
	 */
	
	public String toString() {
		return name.toString();
	}

	public static final Attribute ABBR = HTMLFactory.newAttribute( "abbr" );
	public static final Attribute ACCEPT_CHARSET = HTMLFactory.newAttribute( "accept-charset" );
	public static final Attribute ACCEPT = HTMLFactory.newAttribute( "accept" );
	public static final Attribute ACCESSKEY = HTMLFactory.newAttribute( "accesskey" );
	public static final Attribute ACTION = HTMLFactory.newAttribute( "action" );
	public static final Attribute ALIGN = HTMLFactory.newAttribute( "align" );
	public static final Attribute ALINK = HTMLFactory.newAttribute( "alink" );
	public static final Attribute ALT = HTMLFactory.newAttribute( "alt" );
	public static final Attribute ARCHIVE = HTMLFactory.newAttribute( "archive" );
	public static final Attribute AXIS = HTMLFactory.newAttribute( "axis" );
	public static final Attribute BACKGROUND = HTMLFactory.newAttribute( "background" );
	public static final Attribute BGCOLOR = HTMLFactory.newAttribute( "bgcolor" );
	public static final Attribute BORDER = HTMLFactory.newAttribute( "border" );
	public static final Attribute CELLPADING = HTMLFactory.newAttribute( "cellpading" );
	public static final Attribute CHAR = HTMLFactory.newAttribute( "char" );
	public static final Attribute CHAROFF = HTMLFactory.newAttribute( "charoff" );
	public static final Attribute CHARSET = HTMLFactory.newAttribute( "charset" );
	public static final Attribute CHECKED = HTMLFactory.newAttribute( "checked" );
	public static final Attribute CITE = HTMLFactory.newAttribute( "cite" );
	public static final Attribute CLASS = HTMLFactory.newAttribute( "class" );
	public static final Attribute CLASSID = HTMLFactory.newAttribute( "classid" );
	public static final Attribute CLEAR = HTMLFactory.newAttribute( "clear" );
	public static final Attribute CODE = HTMLFactory.newAttribute( "code" );
	public static final Attribute CODEBASE = HTMLFactory.newAttribute( "codebase" );
	public static final Attribute CODETYPE = HTMLFactory.newAttribute( "codetype" );
	public static final Attribute COLOR = HTMLFactory.newAttribute( "color" );
	public static final Attribute COLS = HTMLFactory.newAttribute( "cols" );
	public static final Attribute COLSPAN = HTMLFactory.newAttribute( "colspan" );
	public static final Attribute COMPACT = HTMLFactory.newAttribute( "compact" );
	public static final Attribute CONTENT = HTMLFactory.newAttribute( "content" );
	public static final Attribute COORDS = HTMLFactory.newAttribute( "coords" );
	public static final Attribute DATA = HTMLFactory.newAttribute( "data" );
	public static final Attribute DATETIME = HTMLFactory.newAttribute( "datetime" );
	public static final Attribute DECLARE = HTMLFactory.newAttribute( "declare" );
	public static final Attribute DEFER = HTMLFactory.newAttribute( "defer" );
	public static final Attribute DIR = HTMLFactory.newAttribute( "dir" );
	public static final Attribute DISABLED = HTMLFactory.newAttribute( "disable" );
	public static final Attribute ENCTYPE = HTMLFactory.newAttribute( "enctype" );
	public static final Attribute FACE = HTMLFactory.newAttribute( "face" );
	public static final Attribute FOR = HTMLFactory.newAttribute( "for" );
	public static final Attribute FRAME = HTMLFactory.newAttribute( "frame" );
	public static final Attribute FRAMEBORDER = HTMLFactory.newAttribute( "frameborder" );
	public static final Attribute HEADERS = HTMLFactory.newAttribute( "headers" );
	public static final Attribute HEIGHT = HTMLFactory.newAttribute( "height" );
	public static final Attribute HREF = HTMLFactory.newAttribute( "href" );
	public static final Attribute HREFLANG = HTMLFactory.newAttribute( "hreflang" );
	public static final Attribute HSPACE = HTMLFactory.newAttribute( "hspace" );
	public static final Attribute HTTP_EQUIV = HTMLFactory.newAttribute( "http-equiv" );
	public static final Attribute ID = HTMLFactory.newAttribute( "id" );
	public static final Attribute ISMAP = HTMLFactory.newAttribute( "ismap" );
	public static final Attribute LABEL = HTMLFactory.newAttribute( "label" );
	public static final Attribute LANG = HTMLFactory.newAttribute( "lang" );
	public static final Attribute LANGUAGE = HTMLFactory.newAttribute( "language" );
	public static final Attribute LINK = HTMLFactory.newAttribute( "link" );
	public static final Attribute LONGDESC = HTMLFactory.newAttribute( "longdesc" );
	public static final Attribute MARGINHEIGHT = HTMLFactory.newAttribute( "marginheight" );
	public static final Attribute MARGINWIDTH = HTMLFactory.newAttribute( "marginwidth" );
	public static final Attribute MARGINLENGTH = HTMLFactory.newAttribute( "marginlength" );
	public static final Attribute MEDIA = HTMLFactory.newAttribute( "media" );
	public static final Attribute METHOD = HTMLFactory.newAttribute( "method" );
	public static final Attribute MULTIPLE = HTMLFactory.newAttribute( "multiple" );
	public static final Attribute NAME = HTMLFactory.newAttribute( "name" );
	public static final Attribute NOHREF = HTMLFactory.newAttribute( "nohref" );
	public static final Attribute NORESIZE = HTMLFactory.newAttribute( "noresize" );
	public static final Attribute NOSHADE = HTMLFactory.newAttribute( "noshade" );
	public static final Attribute NOWRAP = HTMLFactory.newAttribute( "nowrap" );
	public static final Attribute OBJECT = HTMLFactory.newAttribute( "object" );
	public static final Attribute ONBLUR = HTMLFactory.newAttribute( "onblur" );
	public static final Attribute ONCHANGE = HTMLFactory.newAttribute( "onchange" );
	public static final Attribute ONCLICK = HTMLFactory.newAttribute( "onclick" );
	public static final Attribute ONDBLCLICK = HTMLFactory.newAttribute( "ondblclick" );
	public static final Attribute ONFOCUS = HTMLFactory.newAttribute( "onfocus" );
	public static final Attribute ONKEYDOWN = HTMLFactory.newAttribute( "onkeydown" );
	public static final Attribute ONKEYPRESS = HTMLFactory.newAttribute( "onkeypress" );
	public static final Attribute ONKEYUP = HTMLFactory.newAttribute( "onkeyup" );
	public static final Attribute ONLOAD = HTMLFactory.newAttribute( "onload" );
	public static final Attribute ONMOUSEDOWN = HTMLFactory.newAttribute( "onmousedown" );
	public static final Attribute ONMOUSEMOVE = HTMLFactory.newAttribute( "onmousemove" );
	public static final Attribute ONMOUSEOUT = HTMLFactory.newAttribute( "onmouseout" );
	public static final Attribute ONMOUSEOVER = HTMLFactory.newAttribute( "onmouseover" );
	public static final Attribute ONMOUSEUP = HTMLFactory.newAttribute( "ommouseup" );
	public static final Attribute ONRESET = HTMLFactory.newAttribute( "onreset" );
	public static final Attribute ONSELECT = HTMLFactory.newAttribute( "onselest" );
	public static final Attribute ONSUBMIT = HTMLFactory.newAttribute( "onsubmit" );
	public static final Attribute ONUNLOAD = HTMLFactory.newAttribute( "onunload" );
	public static final Attribute PROFILE = HTMLFactory.newAttribute( "profile" );
	public static final Attribute PROMPT = HTMLFactory.newAttribute( "prompt" );
	public static final Attribute READONLY = HTMLFactory.newAttribute( "readonly" );
	public static final Attribute REL = HTMLFactory.newAttribute( "rel" );
	public static final Attribute REV = HTMLFactory.newAttribute( "rev" );
	public static final Attribute ROWS = HTMLFactory.newAttribute( "rows" );
	public static final Attribute ROWSPAN = HTMLFactory.newAttribute( "rowspan" );
	public static final Attribute RULES = HTMLFactory.newAttribute( "rules" );
	public static final Attribute SCHEME = HTMLFactory.newAttribute( "scheme" );
	public static final Attribute SCOPE = HTMLFactory.newAttribute( "scope" );
	public static final Attribute SCROLLING = HTMLFactory.newAttribute( "scrolling" );
	public static final Attribute SELECTED = HTMLFactory.newAttribute( "selected" );
	public static final Attribute SHAPE = HTMLFactory.newAttribute( "shape" );
	public static final Attribute SIZE = HTMLFactory.newAttribute( "size" );
	public static final Attribute SPAN = HTMLFactory.newAttribute( "span" );
	public static final Attribute SRC = HTMLFactory.newAttribute( "src" );
	public static final Attribute STANDBY = HTMLFactory.newAttribute( "stanby" );
	public static final Attribute START = HTMLFactory.newAttribute( "start" );
	public static final Attribute STYLE = HTMLFactory.newAttribute( "style" );
	public static final Attribute SUMMARY = HTMLFactory.newAttribute( "summary" );
	public static final Attribute TABINDEX = HTMLFactory.newAttribute( "tabindex" );
	public static final Attribute TARGET = HTMLFactory.newAttribute( "target" );
	public static final Attribute TEXT = HTMLFactory.newAttribute( "text" );
	public static final Attribute TITLE = HTMLFactory.newAttribute( "title" );
	public static final Attribute TYPE = HTMLFactory.newAttribute( "type" );
	public static final Attribute USEMAP = HTMLFactory.newAttribute( "usemap" );
	public static final Attribute VALIGN = HTMLFactory.newAttribute( "valign" );
	public static final Attribute VALUE = HTMLFactory.newAttribute( "value" );
	public static final Attribute VALUETYPE = HTMLFactory.newAttribute( "valuetype" );
	public static final Attribute VERSION = HTMLFactory.newAttribute( "version" );
	public static final Attribute VLINK = HTMLFactory.newAttribute( "vlink" );
	public static final Attribute VSPACE = HTMLFactory.newAttribute( "vspace" );
	public static final Attribute WIDTH = HTMLFactory.newAttribute( "width" );
	public static final Attribute UNKNOWN = HTMLFactory.newAttribute( "unknown" );
}
