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
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ReferenceLinkedOpenHashSet;
import it.unimi.dsi.lang.MutableString;


/** An HTML element type. */
public final class Element {

    /** The name of the type of this element. */
	public final CharSequence name;
	/** The length of {@link #name}. */
	public final int nameLength;
	/** Whether this element breaks the flow. */
	public final boolean breaksFlow;
	/** Whether this element is simple. */
	public final boolean isSimple;
	/** Whether this element has implicit closure. */
	public final boolean isImplicit;
	/** The content model for this element. */
	final ReferenceLinkedOpenHashSet<Element> contentModel;

	/** Creates a new element with the specified name. 
	 * The element is assumed to break the flow, 
	 * and neither being simple nor having implicit closure.
	 *
	 * @param name the name of the type of the new element.
	 */
	public Element( final CharSequence name ) {
		this( name, true, false, false );
	}

	/** Creates a new element with the specified name and flags.
	 * The element is assumed not to have implicit closure.
	 *
	 * @param name the name of the type of the new element.
	 * @param breaksFlow true if this elements breaks the flow.
	 * @param isSimple true if this element is simple.
	 */
	public Element( final CharSequence name, final boolean breaksFlow, final boolean isSimple ) {
		this( name, breaksFlow, isSimple, false );
	}

	/** Creates a new element.
	 *
	 * @param name the name of the type of the new element.
	 * @param breaksFlow true if this elements breaks the flow.
	 * @param isSimple true if this element is simple.
	 * @param isImplicit true if this element has implicit closure.
	 */
	public Element( final CharSequence name, final boolean breaksFlow, final boolean isSimple, final boolean isImplicit ) {
		this.name = new MutableString( name );
		this.nameLength = name.length();
		this.breaksFlow = breaksFlow;
		this.isSimple = isSimple;
		this.isImplicit = isImplicit;
		this.contentModel = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	}

	/** Returns the name of this element.
	 * @return the name of this element.
	 */
	public String toString() {
		return name.toString();
	}

	/* --- Tag Names ----------------------------------- */
	public static final Element A = HTMLFactory.newElement( "a" );
	public static final Element ABBR = HTMLFactory.newElement( "abbr" );
	public static final Element ACRONYM = HTMLFactory.newElement( "acronym" );
	public static final Element ADDRESS = HTMLFactory.newElement( "address" );
	// deprecated
	public static final Element APPLET = HTMLFactory.newElement( "applet" );
	// forbidden
	public static final Element AREA = HTMLFactory.newElement( "area", true, true );
	// flowMaintainer
	public static final Element B = HTMLFactory.newElement( "b", false, false );
	// forbidden
	public static final Element BASE = HTMLFactory.newElement( "base", true, true, false );
	// flowMaintainer, forbidden, deprecated
	public static final Element BASEFONT = HTMLFactory.newElement( "basefont", false, true );
	public static final Element BDO = HTMLFactory.newElement( "bdo" );
	// flowMaintainer
	public static final Element BIG = HTMLFactory.newElement( "big", false, false );
	public static final Element BLOCKQUOTE = HTMLFactory.newElement( "blockquote" );
	// 2optional --- even opening is optiona
	public static final Element BODY = HTMLFactory.newElement( "body", true, false, true );
	// forbidden
	public static final Element BR = HTMLFactory.newElement( "br", true, true );
	public static final Element BUTTON = HTMLFactory.newElement( "button" );
	public static final Element CAPTION = HTMLFactory.newElement( "caption" );
	/*Deprecated*/  public static final Element CENTER = HTMLFactory.newElement( "center" );
	public static final Element CITE = HTMLFactory.newElement( "cite" );
	// flowMaintainer
	public static final Element CODE = HTMLFactory.newElement( "code", false, false );
	// forbidden
	public static final Element COL = HTMLFactory.newElement( "col", true, true );
	// optional
	public static final Element COLGROUP = HTMLFactory.newElement( "colgroup", true, false, true );
	// optional
	public static final Element DD = HTMLFactory.newElement( "dd", true, false, true );
	public static final Element DEL = HTMLFactory.newElement( "del" );
	public static final Element DFN = HTMLFactory.newElement( "dfn" );
	/*Deprecated*/  public static final Element DIR = HTMLFactory.newElement( "dir" );
	public static final Element DIV = HTMLFactory.newElement( "div" );
	public static final Element DL = HTMLFactory.newElement( "dl" );
	// optional
	public static final Element DT = HTMLFactory.newElement( "dt", true, false, true );
	// flowMaintainer
	public static final Element EM = HTMLFactory.newElement( "em", false, false );
	// Nonstandard
	public static final Element EMBED = HTMLFactory.newElement( "embed", false, false );
	public static final Element FIELDSET = HTMLFactory.newElement( "fieldset" );
	// flowMaintainer
	/*Deprecated*/  public static final Element FONT = HTMLFactory.newElement( "font", false, false );
	public static final Element FORM = HTMLFactory.newElement( "form" );
	// forbidden
	public static final Element FRAME = HTMLFactory.newElement( "frame", true, true );
	public static final Element FRAMESET = HTMLFactory.newElement( "frameset" );
	public static final Element H1 = HTMLFactory.newElement( "h1" );
	public static final Element H2 = HTMLFactory.newElement( "h2" );
	public static final Element H3 = HTMLFactory.newElement( "h3" );
	public static final Element H4 = HTMLFactory.newElement( "h4" );
	public static final Element H5 = HTMLFactory.newElement( "h5" );
	public static final Element H6 = HTMLFactory.newElement( "h6" );
	// 2optional --- even opening is optional
	public static final Element HEAD = HTMLFactory.newElement( "head", true, false, true );
	// forbidden
	public static final Element HR = HTMLFactory.newElement( "hr", true, true );
	// 2optional --- even opening is optional
	public static final Element HTML = HTMLFactory.newElement( "html", true, false, true );
	// flowMaintainer
	public static final Element I = HTMLFactory.newElement( "i", false, false );
	public static final Element IFRAME = HTMLFactory.newElement( "iframe" );
	// flowMaintainer, forbidden
	public static final Element IMG = HTMLFactory.newElement( "img", false, true );
	// forbidden
	public static final Element INPUT = HTMLFactory.newElement( "input", true, true );
	public static final Element INS = HTMLFactory.newElement( "ins" );
	// forbidden, deprecated
	public static final Element ISINDEX = HTMLFactory.newElement( "isindex", true, true );
	public static final Element KBD = HTMLFactory.newElement( "kbd" );
	public static final Element LABEL = HTMLFactory.newElement( "label" );
	public static final Element LEGEND = HTMLFactory.newElement( "legend" );
	// optional
	public static final Element LI = HTMLFactory.newElement( "li", true, false, true );
	// forbidden
	public static final Element LINK = HTMLFactory.newElement( "link", true, true );
	public static final Element MAP = HTMLFactory.newElement( "map" );
	public static final Element MENU = HTMLFactory.newElement( "menu" );
	// forbidden
	public static final Element META = HTMLFactory.newElement( "meta", true, true );
	public static final Element NOFRAMES = HTMLFactory.newElement( "noframes" );
	public static final Element NOSCRIPT = HTMLFactory.newElement( "noscript" );
	public static final Element OBJECT = HTMLFactory.newElement( "object" );
	public static final Element OL = HTMLFactory.newElement( "ol" );
	// optional
	public static final Element OPTION = HTMLFactory.newElement( "option", true, false, true );
	public static final Element OPTGROUP = HTMLFactory.newElement( "optgroup" );
	// optional
	public static final Element P = HTMLFactory.newElement( "p", true, false, true );
	// forbidden
	public static final Element PARAM = HTMLFactory.newElement( "param", true, true );
	public static final Element PRE = HTMLFactory.newElement( "pre" );
	public static final Element Q = HTMLFactory.newElement( "q" );
	// flowMaintainer
	public static final Element SAMP = HTMLFactory.newElement( "samp", false, false );
	public static final Element SCRIPT = HTMLFactory.newElement( "script" );
	public static final Element SELECT = HTMLFactory.newElement( "select" );
	// flowMaintainer
	public static final Element SMALL = HTMLFactory.newElement( "small", false, false );
	// flowMaintainer
	public static final Element SPAN = HTMLFactory.newElement( "span", false, false );
	// flowMaintainer, deprecated
	public static final Element STRIKE = HTMLFactory.newElement( "strike", false, false );
	// flowMaintainer, deprecated
	public static final Element S = HTMLFactory.newElement( "s", false, false );
	// flowMaintainer
	public static final Element STRONG = HTMLFactory.newElement( "strong", false, false );
	public static final Element STYLE = HTMLFactory.newElement( "style" );
	public static final Element SUB = HTMLFactory.newElement( "sub" );
	public static final Element SUP = HTMLFactory.newElement( "sup" );
	public static final Element TABLE = HTMLFactory.newElement( "table" );
	// 2optional --- even opening is optional
	public static final Element TBODY = HTMLFactory.newElement( "tbody", true, false, true );
	// optional
	public static final Element TD = HTMLFactory.newElement( "td", true, false, true );
	public static final Element TEXTAREA = HTMLFactory.newElement( "textarea" );
	// optional
	public static final Element TFOOT = HTMLFactory.newElement( "tfoot", true, false, true );
	// optional
	public static final Element TH = HTMLFactory.newElement( "th", true, false, true );
	// optional
	public static final Element THEAD = HTMLFactory.newElement( "thead", true, false, true );
	public static final Element TITLE = HTMLFactory.newElement( "title" );
	// optional
	public static final Element TR = HTMLFactory.newElement( "tr", true, false, true );
	// flowMaintainer
	public static final Element TT = HTMLFactory.newElement( "tt", false, false );
	// flowMaintainer, deprecated
	public static final Element U = HTMLFactory.newElement( "u", false, false );
	public static final Element UL = HTMLFactory.newElement( "ul" );
	public static final Element VAR = HTMLFactory.newElement( "var" );
	public static final Element UNKNOWN = HTMLFactory.newElement( "unknown" );

	private static final ReferenceLinkedOpenHashSet<Element> HEADING = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> LIST = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> PREFORMATTED = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> FONTSTYLE = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> PHRASE = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> SPECIAL = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> FORM_CONTROL = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> INLINE = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> BLOCK = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> FLOW = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	private static final ReferenceLinkedOpenHashSet<Element> PRE_EXCLUSION = new ReferenceLinkedOpenHashSet<Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	

	static {
		// We define sets for several entities contained in the HTML 4.01 loose DTD (http://www.w3.org/TR/html4/loose.dtd).
		
		/* <!ENTITY % heading "H1|H2|H3|H4|H5|H6">*/
		HEADING.add( Element.H1 );
		HEADING.add( Element.H2 );
		HEADING.add( Element.H3 );
		HEADING.add( Element.H4 );
		HEADING.add( Element.H5 );
		HEADING.add( Element.H6 );
		
		/* <!ENTITY % list "UL | OL |  DIR | MENU"> */
		LIST.add( Element.UL );
		LIST.add( Element.OL );
		LIST.add( Element.DIR );
		LIST.add( Element.MENU );
		
		/* <!ENTITY % preformatted "PRE"> */
		PREFORMATTED.add( Element.PRE );
		
		/*<!ENTITY % fontstyle "TT | I | B | U | S | STRIKE | BIG | SMALL"> */
		FONTSTYLE.add( Element.TT );
		FONTSTYLE.add( Element.I );
		FONTSTYLE.add( Element.B );
		FONTSTYLE.add( Element.U );
		FONTSTYLE.add( Element.S );
		FONTSTYLE.add( Element.STRIKE );
		FONTSTYLE.add( Element.BIG );
		FONTSTYLE.add( Element.SMALL );
		
		/* <!ENTITY % phrase "EM | STRONG | DFN | CODE | SAMP | KBD | VAR | CITE | ABBR | ACRONYM" > */	
		PHRASE.add( Element.EM );
		PHRASE.add( Element.STRONG );
		PHRASE.add( Element.SAMP );
		PHRASE.add( Element.CODE );
		PHRASE.add( Element.KBD );
		PHRASE.add( Element.DFN );
		PHRASE.add( Element.VAR );
		PHRASE.add( Element.CITE );
		PHRASE.add( Element.ABBR );
		PHRASE.add( Element.ACRONYM );
		
		/* <!ENTITY % special "A | IMG | APPLET | OBJECT | FONT | BASEFONT | BR | SCRIPT |
		   MAP | Q | SUB | SUP | SPAN | BDO | IFRAME"> */
		SPECIAL.add( Element.A );
		SPECIAL.add( Element.SPAN );
		SPECIAL.add( Element.FONT );
		SPECIAL.add( Element.IMG );
		SPECIAL.add( Element.APPLET );
		SPECIAL.add( Element.OBJECT );
		SPECIAL.add( Element.BASEFONT );
		SPECIAL.add( Element.BR );
		SPECIAL.add( Element.EMBED );
		SPECIAL.add( Element.SCRIPT );
		SPECIAL.add( Element.MAP );
		SPECIAL.add( Element.Q );
		SPECIAL.add( Element.SUB );
		SPECIAL.add( Element.SUP );
		SPECIAL.add( Element.BDO );
		SPECIAL.add( Element.IFRAME );
		
		/* <!ENTITY % formctrl "INPUT | SELECT | TEXTAREA | LABEL | BUTTON"> */
		FORM_CONTROL.add( Element.INPUT );
		FORM_CONTROL.add( Element.SELECT );
		FORM_CONTROL.add( Element.TEXTAREA );
		FORM_CONTROL.add( Element.LABEL );
		FORM_CONTROL.add( Element.BUTTON );
		
		/* <!ENTITY % inline "#PCDATA | %fontstyle; | %phrase; | %special; | %formctrl;"> */
		INLINE.addAll( PHRASE );
		INLINE.addAll( FONTSTYLE );
		INLINE.addAll( SPECIAL );
		INLINE.addAll( FORM_CONTROL );
		
		/*	<!ENTITY % block "P | %heading; | %list; | %preformatted; | DL | DIV | CENTER |	
		  NOSCRIPT | NOFRAMES | BLOCKQUOTE | FORM | ISINDEX | HR | TABLE | FIELDSET | ADDRESS"> */
		BLOCK.add( Element.P );
		BLOCK.add( Element.DIV );
		BLOCK.add( Element.TABLE );
		BLOCK.add( Element.FORM );
		BLOCK.add( Element.DL );
		BLOCK.add( Element.BLOCKQUOTE );
		BLOCK.add( Element.CENTER );
		BLOCK.add( Element.NOSCRIPT );
		BLOCK.add( Element.NOFRAMES );
		BLOCK.add( Element.ISINDEX );
		BLOCK.add( Element.HR );
		BLOCK.add( Element.FIELDSET );
		BLOCK.add( Element.ADDRESS );
		BLOCK.addAll( HEADING );
		BLOCK.addAll( LIST );
		BLOCK.addAll( PREFORMATTED );				
		
		/* <!ENTITY % flow "%block; | %inline;"> */
		FLOW.addAll( INLINE );
		FLOW.addAll( BLOCK );
		
		/* <!ENTITY % pre.exclusion "IMG|OBJECT|APPLET|BIG|SMALL|SUB|SUP|FONT|BASEFONT"> */
		PRE_EXCLUSION.add( Element.IMG );
		PRE_EXCLUSION.add( Element.OBJECT );
		PRE_EXCLUSION.add( Element.APPLET );
		PRE_EXCLUSION.add( Element.BIG );
		PRE_EXCLUSION.add( Element.SMALL );
		PRE_EXCLUSION.add( Element.SUB );
		PRE_EXCLUSION.add( Element.SUP );
		PRE_EXCLUSION.add( Element.FONT );
		PRE_EXCLUSION.add( Element.BASEFONT );		
	}

	static {
		/* <!ENTITY % fontstyle "TT | I | B | U | S | STRIKE | BIG | SMALL">
		 <!ENTITY % phrase "EM | STRONG | DFN | CODE | SAMP | KBD | VAR | CITE | ABBR | ACRONYM" >
		 <!ELEMENT (%fontstyle;|%phrase;) - - (%inline;)*> */
		Element.ACRONYM.contentModel.addAll( INLINE );
		Element.ABBR.contentModel.addAll( INLINE );
		Element.CITE.contentModel.addAll( INLINE );
		Element.VAR.contentModel.addAll( INLINE );
		Element.KBD.contentModel.addAll( INLINE );
		Element.SAMP.contentModel.addAll( INLINE );
		Element.CODE.contentModel.addAll( INLINE );
		Element.DFN.contentModel.addAll( INLINE );
		Element.STRONG.contentModel.addAll( INLINE );
		Element.EM.contentModel.addAll( INLINE );
		Element.SMALL.contentModel.addAll( INLINE );
		Element.BIG.contentModel.addAll( INLINE );
		Element.STRIKE.contentModel.addAll( INLINE );
		Element.S.contentModel.addAll( INLINE );
		Element.U.contentModel.addAll( INLINE );
		Element.B.contentModel.addAll( INLINE );
		Element.I.contentModel.addAll( INLINE );
		Element.TT.contentModel.addAll( INLINE );
		
		/* <!ELEMENT (SUB|SUP) - - (%inline;)*    -- subscript, superscript --> */
		Element.SUB.contentModel.addAll( INLINE );
		Element.SUP.contentModel.addAll( INLINE );
		
		/* <!ELEMENT SPAN - - (%inline;)*         -- generic language/style container --> */
		Element.SPAN.contentModel.addAll( INLINE );
		
		/* <!ELEMENT BDO - - (%inline;)*          -- I18N BiDi over-ride --> */
		Element.BDO.contentModel.addAll( INLINE );
		
		/* <!ELEMENT BASEFONT - O EMPTY           -- base font size --> */
		// The map is created empty
		
		/* <!ELEMENT FONT - - (%inline;)*         -- local change to font --> */
		Element.FONT.contentModel.addAll( INLINE );
		
		/* <!ELEMENT BR - O EMPTY                 -- forced line break --> */
		// The map is created empty
		
		/* <!ELEMENT BODY O O (%flow;)* +(INS|DEL)-- document body --> */
		Element.BODY.contentModel.addAll( FLOW );
		Element.BODY.contentModel.add( Element.INS );
		Element.BODY.contentModel.add( Element.DEL );
		
		/* <!ELEMENT ADDRESS - - ((%inline;)|P)*  -- information on author --> */
		Element.ADDRESS.contentModel.addAll( INLINE );
		Element.ADDRESS.contentModel.add( Element.P );
		
		/* <!ELEMENT DIV - - (%flow;)*            -- generic language/style container --> */
		Element.DIV.contentModel.addAll( FLOW );
		
		/* <!ELEMENT CENTER - - (%flow;)*         -- shorthand for DIV align=center --> */
		Element.CENTER.contentModel.addAll( FLOW );
		
		/* <!ELEMENT A - - (%inline;)* -(A)       -- anchor --> */
		Element.A.contentModel.addAll( INLINE );
		Element.A.contentModel.remove( Element.A );
		Element.A.contentModel.rehash();
		
		/* <!ELEMENT MAP - - ((%block;) | AREA)+  -- client-side image map --> */
		Element.MAP.contentModel.addAll( BLOCK );
		Element.MAP.contentModel.add( Element.AREA );
		
		/* <!ELEMENT AREA - O EMPTY               -- client-side image map area --> */
		// The map is created empty
		
		/* <!ELEMENT LINK - O EMPTY               -- a media-independent link --> */
		// The map is created empty
		
		/* <!ELEMENT IMG - O EMPTY                -- Embedded image --> */
		// The map is created empty
		
		/* <!ELEMENT OBJECT - - (PARAM | %flow;)* -- generic embedded object --> */
		Element.OBJECT.contentModel.add( Element.PARAM );
		Element.OBJECT.contentModel.addAll( FLOW );
		
		/* <!ELEMENT PARAM - O EMPTY              -- named property value --> */
		// The map is created empty
		
		/* <!ELEMENT APPLET - - (PARAM | %flow;)* -- Java applet --> */
		Element.APPLET.contentModel.add( Element.PARAM );
		Element.APPLET.contentModel.addAll( FLOW );
		
		/* <!ELEMENT HR - O EMPTY                 -- horizontal rule --> */
		// The map is created empty
		
		/* <!ELEMENT P - O (%inline;)*            -- paragraph --> */
		Element.P.contentModel.addAll( INLINE );
		
		/* <!ELEMENT (%heading;)  - - (%inline;)* -- heading --> */
		/* <!ENTITY % heading "H1|H2|H3|H4|H5|H6">*/
		Element.H6.contentModel.addAll( INLINE );
		Element.H5.contentModel.addAll( INLINE );
		Element.H4.contentModel.addAll( INLINE );
		Element.H3.contentModel.addAll( INLINE );
		Element.H2.contentModel.addAll( INLINE );
		Element.H1.contentModel.addAll( INLINE );
		
		/* <!ELEMENT PRE - - (%inline;)* -(%pre.exclusion;) -- preformatted text --> */
		Element.PRE.contentModel.addAll( INLINE );
		Element.PRE.contentModel.removeAll( PRE_EXCLUSION );
		Element.PRE.contentModel.rehash();
		
		/* <!ELEMENT Q - - (%inline;)*            -- short inline quotation --> */
		Element.Q.contentModel.addAll( INLINE );
		
		/* <!ELEMENT BLOCKQUOTE - - (%flow;)*     -- long quotation --> */
		Element.BLOCKQUOTE.contentModel.addAll( FLOW );
		
		/* <!ELEMENT (INS|DEL) - - (%flow;)*      -- inserted text, deleted text --> */
		Element.INS.contentModel.addAll( FLOW );
		Element.DEL.contentModel.addAll( FLOW );
		
		/* <!ELEMENT DL - - (DT|DD)+              -- definition list --> */
		Element.DL.contentModel.add( Element.DT );
		Element.DL.contentModel.add( Element.DD );
		
		/* <!ELEMENT DT - O (%inline;)*           -- definition term --> */
		Element.DT.contentModel.addAll( INLINE );
		
		/* <!ELEMENT DD - O (%flow;)*             -- definition description --> */
		Element.DD.contentModel.addAll( FLOW );
		
		/* <!ELEMENT OL - - (LI)+                 -- ordered list --> */
		Element.OL.contentModel.add( Element.LI );
		
		/* <!ELEMENT UL - - (LI)+                 -- unordered list --> */
		Element.UL.contentModel.add( Element.LI );
		
		/* <!ELEMENT (DIR|MENU) - - (LI)+ -(%block;) -- directory list, menu list --> */
		Element.DIR.contentModel.add( Element.LI );
		Element.DIR.contentModel.removeAll( BLOCK );
		Element.DIR.contentModel.rehash();
		Element.MENU.contentModel.addAll( Element.DIR.contentModel );
		
		/* <!ELEMENT LI - O (%flow;)*             -- list item --> */
		Element.LI.contentModel.addAll( FLOW );
		
		/* <!ELEMENT FORM - - (%flow;)* -(FORM)   -- interactive form --> */
		Element.FORM.contentModel.addAll( FLOW );
		Element.FORM.contentModel.remove( Element.FORM );
		Element.FORM.contentModel.rehash();
		
		/* <!ELEMENT LABEL - - (%inline;)* -(LABEL) -- form field label text --> */
		Element.LABEL.contentModel.addAll( INLINE );
		Element.LABEL.contentModel.remove( Element.LABEL );
		Element.LABEL.contentModel.rehash();
		
		/* <!ELEMENT INPUT - O EMPTY              -- form control --> */
		// The map is created empty
		
		/* <!ELEMENT SELECT - - (OPTGROUP|OPTION)+ -- option selector --> */
		Element.SELECT.contentModel.add( Element.OPTION );
		Element.SELECT.contentModel.add( Element.OPTGROUP );
		
		/* <!ELEMENT OPTGROUP - - (OPTION)+ -- option group --> */
		Element.OPTGROUP.contentModel.add( Element.OPTION );
		
		/* <!ELEMENT OPTION - O (#PCDATA)         -- selectable choice --> */
		// The map is created empty
		
		/* <!ELEMENT TEXTAREA - - (#PCDATA)       -- multi-line text field --> */
		// The map is created empty
		
		/* <!ELEMENT FIELDSET - - (#PCDATA,LEGEND,(%flow;)*) -- form control group --> */
		Element.FIELDSET.contentModel.addAll( FLOW );
		
		/* <!ELEMENT LEGEND - - (%inline;)*       -- fieldset legend --> */
		Element.LEGEND.contentModel.addAll( INLINE );
		
		/* <!ELEMENT BUTTON - - (%flow;)* -(A|%formctrl;|FORM|ISINDEX|FIELDSET|IFRAME) -- push button --> */
		Element.BUTTON.contentModel.addAll( FLOW );
		Element.BUTTON.contentModel.removeAll( FORM_CONTROL );
		Element.BUTTON.contentModel.remove( Element.A );
		Element.BUTTON.contentModel.remove( Element.FORM );
		Element.BUTTON.contentModel.remove( Element.ISINDEX );
		Element.BUTTON.contentModel.remove( Element.FIELDSET );
		Element.BUTTON.contentModel.remove( Element.IFRAME );
		Element.BUTTON.contentModel.rehash();
		
		/* <!ELEMENT TABLE - -     (CAPTION?, (COL*|COLGROUP*), THEAD?, TFOOT?, TBODY+)> */
		Element.TABLE.contentModel.add( Element.TBODY );
		Element.TABLE.contentModel.add( Element.THEAD );
		Element.TABLE.contentModel.add( Element.TFOOT );
		Element.TABLE.contentModel.add( Element.COL );
		Element.TABLE.contentModel.add( Element.COLGROUP );
		Element.TABLE.contentModel.add( Element.CAPTION );
		
		/* <!ELEMENT CAPTION  - - (%inline;)*     -- table caption --> */
		Element.CAPTION.contentModel.addAll( INLINE );
		
		/* <!ELEMENT THEAD    - O (TR)+           -- table header --> */
		Element.THEAD.contentModel.add( Element.TR );
		
		/* <!ELEMENT TFOOT    - O (TR)+           -- table footer --> */
		Element.TFOOT.contentModel.add( Element.TR );
		
		/* <!ELEMENT TBODY    O O (TR)+           -- table body --> */
		Element.TBODY.contentModel.add( Element.TR );
		
		/* <!ELEMENT COLGROUP - O (COL)*          -- table column group --> */
		Element.COLGROUP.contentModel.add( Element.COL );
		
		/* <!ELEMENT COL      - O EMPTY           -- table column --> */
		// The map is created empty
		
		/* <!ELEMENT TR       - O (TH|TD)+        -- table row --> */
		Element.TR.contentModel.add( Element.TD );
		Element.TR.contentModel.add( Element.TH );
		
		/* <!ELEMENT (TH|TD)  - O (%flow;)*       -- table header cell, table data cell--> */
		Element.TH.contentModel.addAll( FLOW );
		Element.TD.contentModel.addAll( FLOW );
		
		/* <!ELEMENT FRAMESET - - ((FRAMESET|FRAME)+ & NOFRAMES?) -- window subdivision--> */
		Element.FRAMESET.contentModel.add( Element.FRAME );
		Element.FRAMESET.contentModel.add( Element.FRAMESET );
		Element.FRAMESET.contentModel.add( Element.NOFRAMES );
		
		/* <!ELEMENT FRAME - O EMPTY              -- subwindow --> */
		// The map is created empty
		
		/* <!ELEMENT IFRAME - - (%flow;)*         -- inline subwindow --> */
		Element.IFRAME.contentModel.addAll( FLOW );
		
		/* Nonstandard */
		Element.EMBED.contentModel.addAll( INLINE );
		Element.EMBED.contentModel.addAll( BLOCK );

		/* <![ %HTML.Frameset; [<!ENTITY % noframes.content "(BODY) -(NOFRAMES)">]]>
		 <!ENTITY % noframes.content "(%flow;)*">
		 <!ELEMENT NOFRAMES - - %noframes.content; -- alternate content container for non frame-based rendering --> */
		Element.NOFRAMES.contentModel.addAll( FLOW );
		Element.NOFRAMES.contentModel.remove( Element.NOFRAMES );
		Element.NOFRAMES.contentModel.rehash();
		
		/* <!-- %head.misc; defined earlier on as "SCRIPT|STYLE|META|LINK|OBJECT" -->
		 <!ENTITY % head.content "TITLE & ISINDEX? & BASE?">
		 <!ELEMENT HEAD O O (%head.content;) +(%head.misc;) -- document head --> */
		Element.HEAD.contentModel.add( Element.SCRIPT );
		Element.HEAD.contentModel.add( Element.STYLE );
		Element.HEAD.contentModel.add( Element.META );
		Element.HEAD.contentModel.add( Element.LINK );
		Element.HEAD.contentModel.add( Element.OBJECT );
		Element.HEAD.contentModel.add( Element.TITLE );
		Element.HEAD.contentModel.add( Element.ISINDEX );
		Element.HEAD.contentModel.add( Element.BASE );
		
		/* <!ELEMENT TITLE - - (#PCDATA) -(%head.misc;) -- document title --> */
		// The map is created empty
		
		/* <!ELEMENT ISINDEX - O EMPTY            -- single line prompt --> */
		// The map is created empty
		
		/* <!ELEMENT BASE - O EMPTY               -- document base URI --> */
		// The map is created empty
		
		/* <!ELEMENT META - O EMPTY               -- generic metainformation --> */
		// The map is created empty
		
		/* <!ELEMENT STYLE - - %StyleSheet        -- style info --> */
		// The map is created empty
		
		/* <!ELEMENT SCRIPT - - %Script;          -- script statements --> */
		// The map is created empty
		
		/* <!ELEMENT NOSCRIPT - - (%flow;)*  -- alternate content container for non script-based rendering --> */
		Element.NOSCRIPT.contentModel.addAll( FLOW );
		
		/* <![ %HTML.Frameset; [<!ENTITY % html.content "HEAD, FRAMESET">]]>
		 <!ENTITY % html.content "HEAD, BODY">
		 <!ELEMENT HTML O O (%html.content;)    -- document root element --> */
		Element.HTML.contentModel.add( Element.BODY );
		Element.HTML.contentModel.add( Element.HEAD );
		Element.HTML.contentModel.add( Element.FRAMESET );
	}

}
