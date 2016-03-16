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
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;

/** A parsing factory for (X)HTML. 
 * 
 * <p><strong>Warning:</strong> for maximum flexibility, the methods of this factory
 * do <em>not</em> perform case normalisation. If you are parsing HTML, you are invited
 * to downcase your names before accessing {@link #getElement(MutableString)} 
 * and {@link #getAttribute(MutableString)}.
 * 
 * <p>This class is a singleton, and its only instance is accessible using the public field 
 * {@link #INSTANCE}.
 * 
 * <p>The relationship between this class and {@link Element}/{@link Attribute} is a bit
 * twisted due to the need to accomodate two features:
 * <ul>
 * <li>(X)HTML interned objects must be accessible directly (see, e.g., {@link Element#A});
 * <li>(X)HTML interned objects must be put into suitable name-to-object maps.
 * </ul>
 * 
 * <p>To this purpose, this class exports packagewise some static factory methods that create {@link Element}s and
 * {@link Attribute}s and register them locally. The static initialisation code in 
 * {@link Element} and {@link Attribute} creates elements such as {@link Element#A} using the abovementioned
 * factory methods.
 * 
 * <p>An alternative implementation could use reflection, but I don't see great advantages.
 */

public class HTMLFactory implements ParsingFactory {

	private HTMLFactory() {}
	
	public static final HTMLFactory INSTANCE = new HTMLFactory();
	
	public Element getElement( final MutableString name ) {
		return NAME2ELEMENT.get( name );
	}

	public Attribute getAttribute( final MutableString name ) {
		return NAME2ATTRIBUTE.get( name );
	}

	public Entity getEntity( final MutableString name ) {
		return NAME2ENTITY.get( name );
	}

	/** A (quick) map from entity names to entites. */
    static final Object2ObjectOpenHashMap<CharSequence,Entity> NAME2ENTITY = new Object2ObjectOpenHashMap<CharSequence,Entity>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	
	/** A (quick) map from attribute names to attributes. */
    static final Object2ObjectOpenHashMap<CharSequence,Attribute> NAME2ATTRIBUTE = new Object2ObjectOpenHashMap<CharSequence,Attribute>( Hash.DEFAULT_INITIAL_SIZE, .5f );

    /** A (quick) map from element-type names to element types. */
    static final Object2ObjectOpenHashMap<CharSequence,Element> NAME2ELEMENT = new Object2ObjectOpenHashMap<CharSequence,Element>( Hash.DEFAULT_INITIAL_SIZE, .5f );
	
	static Element newElement( final CharSequence name ) {
		final Element element = new Element( name );
		NAME2ELEMENT.put( element.name, element );
		return element;
	}

	static Element newElement( final CharSequence name, final boolean breaksFlow, final boolean isSimple ) {
		final Element element = new Element( name, breaksFlow, isSimple );
		NAME2ELEMENT.put( element.name, element );
		return element;
	}

	static Element newElement( final CharSequence name, final boolean breaksFlow, final boolean isSimple, final boolean isImplicit ) {
		final Element element = new Element( name, breaksFlow, isSimple, isImplicit );
		NAME2ELEMENT.put( element.name, element );
		return element;
	}

	static Attribute newAttribute( final CharSequence name ) {
		final Attribute attribute = new Attribute( name );
		NAME2ATTRIBUTE.put( attribute.name, attribute );
		return attribute;
	}

	static Entity newEntity( final CharSequence name, final char c ) {
		final Entity entity = new Entity( name, c );
		NAME2ENTITY.put( entity.name, entity );
		return entity;
	}
    static {
		NAME2ATTRIBUTE.defaultReturnValue( Attribute.UNKNOWN );
        NAME2ELEMENT.defaultReturnValue( Element.UNKNOWN );

		// --- Entity Names -----------------------------------
		
		// Latin 1
		HTMLFactory.newEntity( "nbsp", (char)160 );
		HTMLFactory.newEntity( "iexcl", (char)161 );
		HTMLFactory.newEntity( "cent", (char)162 );
		HTMLFactory.newEntity( "pound", (char)163 );
		HTMLFactory.newEntity( "curren", (char)164 );
		HTMLFactory.newEntity( "yen", (char)165 );
		HTMLFactory.newEntity( "brvbar", (char)166 );
		HTMLFactory.newEntity( "sect", (char)167 );
		HTMLFactory.newEntity( "uml", (char)168 );
		HTMLFactory.newEntity( "copy", (char)169 );
		HTMLFactory.newEntity( "ordf", (char)170 );
		HTMLFactory.newEntity( "laquo", (char)171 );
		HTMLFactory.newEntity( "not", (char)172 );
		HTMLFactory.newEntity( "shy", (char)173 );
		HTMLFactory.newEntity( "reg", (char)174 );
		HTMLFactory.newEntity( "macr", (char)175 );
		HTMLFactory.newEntity( "deg", (char)176 );
		HTMLFactory.newEntity( "plusmn", (char)177 );
		HTMLFactory.newEntity( "sup2", (char)178 );
		HTMLFactory.newEntity( "sup3", (char)179 );
		HTMLFactory.newEntity( "acute", (char)180 );
		HTMLFactory.newEntity( "micro", (char)181 );
		HTMLFactory.newEntity( "para", (char)182 );
		HTMLFactory.newEntity( "middot", (char)183 );
		HTMLFactory.newEntity( "cedil", (char)184 );
		HTMLFactory.newEntity( "sup1", (char)185 );
		HTMLFactory.newEntity( "ordm", (char)186 );
		HTMLFactory.newEntity( "raquo", (char)187 );
		HTMLFactory.newEntity( "frac14", (char)188 );
		HTMLFactory.newEntity( "frac12", (char)189 );
		HTMLFactory.newEntity( "frac34", (char)190 );
		HTMLFactory.newEntity( "iquest", (char)191 );
		HTMLFactory.newEntity( "Agrave", (char)192 );
		HTMLFactory.newEntity( "Aacute", (char)193 );
		HTMLFactory.newEntity( "Acirc", (char)194 );
		HTMLFactory.newEntity( "Atilde", (char)195 );
		HTMLFactory.newEntity( "Auml", (char)196 );
		HTMLFactory.newEntity( "Aring", (char)197 );
		HTMLFactory.newEntity( "AElig", (char)198 );
		HTMLFactory.newEntity( "Ccedil", (char)199 );
		HTMLFactory.newEntity( "Egrave", (char)200 );
		HTMLFactory.newEntity( "Eacute", (char)201 );
		HTMLFactory.newEntity( "Ecirc", (char)202 );
		HTMLFactory.newEntity( "Euml", (char)203 );
		HTMLFactory.newEntity( "Igrave", (char)204 );
		HTMLFactory.newEntity( "Iacute", (char)205 );
		HTMLFactory.newEntity( "Icirc", (char)206 );
		HTMLFactory.newEntity( "Iuml", (char)207 );
		HTMLFactory.newEntity( "ETH", (char)208 );
		HTMLFactory.newEntity( "Ntilde", (char)209 );
		HTMLFactory.newEntity( "Ograve", (char)210 );
		HTMLFactory.newEntity( "Oacute", (char)211 );
		HTMLFactory.newEntity( "Ocirc", (char)212 );
		HTMLFactory.newEntity( "Otilde", (char)213 );
		HTMLFactory.newEntity( "Ouml", (char)214 );
		HTMLFactory.newEntity( "times", (char)215 );
		HTMLFactory.newEntity( "Oslash", (char)216 );
		HTMLFactory.newEntity( "Ugrave", (char)217 );
		HTMLFactory.newEntity( "Uacute", (char)218 );
		HTMLFactory.newEntity( "Ucirc", (char)219 );
		HTMLFactory.newEntity( "Uuml", (char)220 );
		HTMLFactory.newEntity( "Yacute", (char)221 );
		HTMLFactory.newEntity( "THORN", (char)222 );
		HTMLFactory.newEntity( "szlig", (char)223 );
		HTMLFactory.newEntity( "agrave", (char)224 );
		HTMLFactory.newEntity( "aacute", (char)225 );
		HTMLFactory.newEntity( "acirc", (char)226 );
		HTMLFactory.newEntity( "atilde", (char)227 );
		HTMLFactory.newEntity( "auml", (char)228 );
		HTMLFactory.newEntity( "aring", (char)229 );
		HTMLFactory.newEntity( "aelig", (char)230 );
		HTMLFactory.newEntity( "ccedil", (char)231 );
		HTMLFactory.newEntity( "egrave", (char)232 );
		HTMLFactory.newEntity( "eacute", (char)233 );
		HTMLFactory.newEntity( "ecirc", (char)234 );
		HTMLFactory.newEntity( "euml", (char)235 );
		HTMLFactory.newEntity( "igrave", (char)236 );
		HTMLFactory.newEntity( "iacute", (char)237 );
		HTMLFactory.newEntity( "icirc", (char)238 );
		HTMLFactory.newEntity( "iuml", (char)239 );
		HTMLFactory.newEntity( "eth", (char)240 );
		HTMLFactory.newEntity( "ntilde", (char)241 );
		HTMLFactory.newEntity( "ograve", (char)242 );
		HTMLFactory.newEntity( "oacute", (char)243 );
		HTMLFactory.newEntity( "ocirc", (char)244 );
		HTMLFactory.newEntity( "otilde", (char)245 );
		HTMLFactory.newEntity( "ouml", (char)246 );
		HTMLFactory.newEntity( "divide", (char)247 );
		HTMLFactory.newEntity( "oslash", (char)248 );
		HTMLFactory.newEntity( "ugrave", (char)249 );
		HTMLFactory.newEntity( "uacute", (char)250 );
		HTMLFactory.newEntity( "ucirc", (char)251 );
		HTMLFactory.newEntity( "uuml", (char)252 );
		HTMLFactory.newEntity( "yacute", (char)253 );
		HTMLFactory.newEntity( "thorn", (char)254 );
		HTMLFactory.newEntity( "yuml", (char)255 );
		
		// Special
		HTMLFactory.newEntity( "quot", (char)34 );
		HTMLFactory.newEntity( "apos", (char)39 );
		HTMLFactory.newEntity( "amp", (char)38 );
		HTMLFactory.newEntity( "lt", (char)60 );
		HTMLFactory.newEntity( "gt", (char)62 );
		HTMLFactory.newEntity( "OElig", (char)338 );
		HTMLFactory.newEntity( "oelig", (char)339 );
		HTMLFactory.newEntity( "Scaron", (char)352 );
		HTMLFactory.newEntity( "scaron", (char)353 );
		HTMLFactory.newEntity( "Yuml", (char)376 );
		HTMLFactory.newEntity( "circ", (char)710 );
		HTMLFactory.newEntity( "tilde", (char)732 );
		HTMLFactory.newEntity( "ensp", (char)8194 );
		HTMLFactory.newEntity( "emsp", (char)8195 );
		HTMLFactory.newEntity( "thinsp", (char)8201 );
		HTMLFactory.newEntity( "zwnj", (char)8204 );
		HTMLFactory.newEntity( "zwj", (char)8205 );
		HTMLFactory.newEntity( "lrm", (char)8206 );
		HTMLFactory.newEntity( "rlm", (char)8207 );
		HTMLFactory.newEntity( "ndash", (char)8211 );
		HTMLFactory.newEntity( "mdash", (char)8212 );
		HTMLFactory.newEntity( "lsquo", (char)8216 );
		HTMLFactory.newEntity( "rsquo", (char)8217 );
		HTMLFactory.newEntity( "sbquo", (char)8218 );
		HTMLFactory.newEntity( "ldquo", (char)8220 );
		HTMLFactory.newEntity( "rdquo", (char)8221 );
		HTMLFactory.newEntity( "bdquo", (char)8222 );
		HTMLFactory.newEntity( "dagger", (char)8224 );
		HTMLFactory.newEntity( "Dagger", (char)8225 );
		HTMLFactory.newEntity( "permil", (char)8240 );
		HTMLFactory.newEntity( "lsaquo", (char)8249 );
		HTMLFactory.newEntity( "rsaquo", (char)8250 );
		HTMLFactory.newEntity( "euro", (char)8364 );
		
		// Symbols
		HTMLFactory.newEntity( "fnof", (char)402 );
		HTMLFactory.newEntity( "Alpha", (char)913 );
		HTMLFactory.newEntity( "Beta", (char)914 );
		HTMLFactory.newEntity( "Gamma", (char)915 );
		HTMLFactory.newEntity( "Delta", (char)916 );
		HTMLFactory.newEntity( "Epsilon", (char)917 );
		HTMLFactory.newEntity( "Zeta", (char)918 );
		HTMLFactory.newEntity( "Eta", (char)919 );
		HTMLFactory.newEntity( "Theta", (char)920 );
		HTMLFactory.newEntity( "Iota", (char)921 );
		HTMLFactory.newEntity( "Kappa", (char)922 );
		HTMLFactory.newEntity( "Lambda", (char)923 );
		HTMLFactory.newEntity( "Mu", (char)924 );
		HTMLFactory.newEntity( "Nu", (char)925 );
		HTMLFactory.newEntity( "Xi", (char)926 );
		HTMLFactory.newEntity( "Omicron", (char)927 );
		HTMLFactory.newEntity( "Pi", (char)928 );
		HTMLFactory.newEntity( "Rho", (char)929 );
		HTMLFactory.newEntity( "Sigma", (char)931 );
		HTMLFactory.newEntity( "Tau", (char)932 );
		HTMLFactory.newEntity( "Upsilon", (char)933 );
		HTMLFactory.newEntity( "Phi", (char)934 );
		HTMLFactory.newEntity( "Chi", (char)935 );
		HTMLFactory.newEntity( "Psi", (char)936 );
		HTMLFactory.newEntity( "Omega", (char)937 );
		HTMLFactory.newEntity( "alpha", (char)945 );
		HTMLFactory.newEntity( "beta", (char)946 );
		HTMLFactory.newEntity( "gamma", (char)947 );
		HTMLFactory.newEntity( "delta", (char)948 );
		HTMLFactory.newEntity( "epsilon", (char)949 );
		HTMLFactory.newEntity( "zeta", (char)950 );
		HTMLFactory.newEntity( "eta", (char)951 );
		HTMLFactory.newEntity( "theta", (char)952 );
		HTMLFactory.newEntity( "iota", (char)953 );
		HTMLFactory.newEntity( "kappa", (char)954 );
		HTMLFactory.newEntity( "lambda", (char)955 );
		HTMLFactory.newEntity( "mu", (char)956 );
		HTMLFactory.newEntity( "nu", (char)957 );
		HTMLFactory.newEntity( "xi", (char)958 );
		HTMLFactory.newEntity( "omicron", (char)959 );
		HTMLFactory.newEntity( "pi", (char)960 );
		HTMLFactory.newEntity( "rho", (char)961 );
		HTMLFactory.newEntity( "sigmaf", (char)962 );
		HTMLFactory.newEntity( "sigma", (char)963 );
		HTMLFactory.newEntity( "tau", (char)964 );
		HTMLFactory.newEntity( "upsilon", (char)965 );
		HTMLFactory.newEntity( "phi", (char)966 );
		HTMLFactory.newEntity( "chi", (char)967 );
		HTMLFactory.newEntity( "psi", (char)968 );
		HTMLFactory.newEntity( "omega", (char)969 );
		HTMLFactory.newEntity( "thetasym", (char)977 );
		HTMLFactory.newEntity( "upsih", (char)978 );
		HTMLFactory.newEntity( "piv", (char)982 );
		HTMLFactory.newEntity( "bull", (char)8226 );
		HTMLFactory.newEntity( "hellip", (char)8230 );
		HTMLFactory.newEntity( "prime", (char)8242 );
		HTMLFactory.newEntity( "Prime", (char)8243 );
		HTMLFactory.newEntity( "oline", (char)8254 );
		HTMLFactory.newEntity( "frasl", (char)8260 );
		HTMLFactory.newEntity( "weierp", (char)8472 );
		HTMLFactory.newEntity( "image", (char)8465 );
		HTMLFactory.newEntity( "real", (char)8476 );
		HTMLFactory.newEntity( "trade", (char)8482 );
		HTMLFactory.newEntity( "alefsym", (char)8501 );
		HTMLFactory.newEntity( "larr", (char)8592 );
		HTMLFactory.newEntity( "uarr", (char)8593 );
		HTMLFactory.newEntity( "rarr", (char)8594 );
		HTMLFactory.newEntity( "darr", (char)8595 );
		HTMLFactory.newEntity( "harr", (char)8596 );
		HTMLFactory.newEntity( "crarr", (char)8629 );
		HTMLFactory.newEntity( "lArr", (char)8656 );
		HTMLFactory.newEntity( "uArr", (char)8657 );
		HTMLFactory.newEntity( "rArr", (char)8658 );
		HTMLFactory.newEntity( "dArr", (char)8659 );
		HTMLFactory.newEntity( "hArr", (char)8660 );
		HTMLFactory.newEntity( "forall", (char)8704 );
		HTMLFactory.newEntity( "part", (char)8706 );
		HTMLFactory.newEntity( "exist", (char)8707 );
		HTMLFactory.newEntity( "empty", (char)8709 );
		HTMLFactory.newEntity( "nabla", (char)8711 );
		HTMLFactory.newEntity( "isin", (char)8712 );
		HTMLFactory.newEntity( "notin", (char)8713 );
		HTMLFactory.newEntity( "ni", (char)8715 );
		HTMLFactory.newEntity( "prod", (char)8719 );
		HTMLFactory.newEntity( "sum", (char)8721 );
		HTMLFactory.newEntity( "minus", (char)8722 );
		HTMLFactory.newEntity( "lowast", (char)8727 );
		HTMLFactory.newEntity( "radic", (char)8730 );
		HTMLFactory.newEntity( "prop", (char)8733 );
		HTMLFactory.newEntity( "infin", (char)8734 );
		HTMLFactory.newEntity( "ang", (char)8736 );
		HTMLFactory.newEntity( "and", (char)8743 );
		HTMLFactory.newEntity( "or", (char)8744 );
		HTMLFactory.newEntity( "cap", (char)8745 );
		HTMLFactory.newEntity( "cup", (char)8746 );
		HTMLFactory.newEntity( "int", (char)8747 );
		HTMLFactory.newEntity( "there4", (char)8756 );
		HTMLFactory.newEntity( "sim", (char)8764 );
		HTMLFactory.newEntity( "cong", (char)8773 );
		HTMLFactory.newEntity( "asymp", (char)8776 );
		HTMLFactory.newEntity( "ne", (char)8800 );
		HTMLFactory.newEntity( "equiv", (char)8801 );
		HTMLFactory.newEntity( "le", (char)8804 );
		HTMLFactory.newEntity( "ge", (char)8805 );
		HTMLFactory.newEntity( "sub", (char)8834 );
		HTMLFactory.newEntity( "sup", (char)8835 );
		HTMLFactory.newEntity( "nsub", (char)8836 );
		HTMLFactory.newEntity( "sube", (char)8838 );
		HTMLFactory.newEntity( "supe", (char)8839 );
		HTMLFactory.newEntity( "oplus", (char)8853 );
		HTMLFactory.newEntity( "otimes", (char)8855 );
		HTMLFactory.newEntity( "perp", (char)8869 );
		HTMLFactory.newEntity( "sdot", (char)8901 );
		HTMLFactory.newEntity( "lceil", (char)8968 );
		HTMLFactory.newEntity( "rceil", (char)8969 );
		HTMLFactory.newEntity( "lfloor", (char)8970 );
		HTMLFactory.newEntity( "rfloor", (char)8971 );
		HTMLFactory.newEntity( "lang", (char)9001 );
		HTMLFactory.newEntity( "rang", (char)9002 );
		HTMLFactory.newEntity( "loz", (char)9674 );
		HTMLFactory.newEntity( "spades", (char)9824 );
		HTMLFactory.newEntity( "clubs", (char)9827 );
		HTMLFactory.newEntity( "hearts", (char)9829 );
		HTMLFactory.newEntity( "diams", (char)9830 );
	}
}
