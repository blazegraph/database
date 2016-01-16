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
 * Created on May 6, 2014 by Jeremy J. Carroll, Syapse Inc.
 */
package com.bigdata.search;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

/**
 * This class can be used with the bigdata properties file to specify
 * which {@link Analyzer}s are used for which languages.
 * Languages are specified by the language tag on RDF literals, which conform
 * with <a href="http://www.rfc-editor.org/rfc/rfc5646.txt">RFC 5646</a>.
 * Within bigdata plain literals are assigned to the default locale's language. 
 * 
 * The bigdata properties are used to map language ranges, as specified by 
 * <a href="http://www.rfc-editor.org/rfc/rfc4647.txt">RFC 4647</a> to classes which extend {@link Analyzer}.
 * Supported classes included all the natural language specific classes from Lucene, and also:
 * <ul>
 * <li>{@link PatternAnalyzer}
 * <li>{@link TermCompletionAnalyzer}
 * <li>{@link KeywordAnalyzer}
 * <li>{@link SimpleAnalyzer}
 * <li>{@link StopAnalyzer}
 * <li>{@link WhitespaceAnalyzer}
 * <li>{@link StandardAnalyzer}
 * </ul> 
 * More generally any subclass of  {@link Analyzer} that has at least one constructor matching:
 * <ul>
 * <li>no arguments
 * <li>{@link Version}
 * <li>{@link Version}, {@link Set}
 * </ul>
 * is usable. If the class has a static method named <code>getDefaultStopSet()</code> then this is assumed
 * to do what it says on the can; some of the Lucene analyzers store their default stop words elsewhere,
 * and such stopwords are usable by this class. If no stop word set can be found, and there is a constructor without
 * stopwords and a constructor with stopwords, then the former is assumed to use a default stop word set.
 * <p>
 * Configuration is by means of the bigdata properties file.
 * All relevant properties start <code>com.bigdata.search.ConfigurableAnalyzerFactory</code> which we 
 * abbreviate to <code>c.b.s.C</code> in this documentation. 
 * Properties from {@link Options} apply to the factory.
 * <p>
 * Other properties, from {@link AnalyzerOptions} start with
 * <code>c.b.s.C.analyzer.<em>language-range</em></code> where <code><em>language-range</em></code> conforms
 * with the extended language range construct from RFC 4647, section 2.2. 
 * There is an issue that bigdata does not allow '*' in property names, and we use the character '_' to
 * substitute for '*' in extended language ranges in property names.
 * These are used to specify an analyzer for the given language range.
 * <p>
 * If no analyzer is specified for the language range <code>*</code> then the {@link StandardAnalyzer} is used.
 * <p>
 * Given any specific language, then the analyzer matching the longest configured language range, 
 * measured in number of subtags is returned by {@link #getAnalyzer(String, boolean)} 
 * In the event of a tie, the alphabetically first language range is used.
 * The algorithm to find a match is "Extended Filtering" as defined in section 3.3.2 of RFC 4647.
 * <p>
 * Some useful analyzers are as follows:
 * <dl>
 * <dt>{@link KeywordAnalyzer}</dt>
 * <dd>This treats every lexical value as a single search token</dd>
 * <dt>{@link WhitespaceAnalyzer}</dt>
 * <dd>This uses whitespace to tokenize</dd>
 * <dt>{@link PatternAnalyzer}</dt>
 * <dd>This uses a regular expression to tokenize</dd>
 * <dt>{@link TermCompletionAnalyzer}</dt>
 * <dd>This uses up to three regular expressions to specify multiple tokens for each word, to address term completion use cases.</dd>
 * <dt>{@link EmptyAnalyzer}</dt>
 * <dd>This suppresses the functionality, by treating every expression as a stop word.</dd>
 * </dl>
 * there are in addition the language specific analyzers that are included
 * by using the option {@link Options#NATURAL_LANGUAGE_SUPPORT}
 * 
 * 
 * @author jeremycarroll
 *
 */
public class ConfigurableAnalyzerFactory implements IAnalyzerFactory {
	final private static transient Logger log = Logger.getLogger(ConfigurableAnalyzerFactory.class);

	/**
     * Options understood by the {@link ConfigurableAnalyzerFactory}.
     */
    public interface Options {
    	/**
    	 * By setting this option to true, then all the known Lucene Analyzers for natural
    	 * languages are used for a range of language tags.
    	 * These settings may then be overridden by the settings of the user.
    	 * Specifically the following properties are loaded, prior to loading the
    	 * user's specification (with <code>c.b.s.C</code> expanding to 
    	 * <code>com.bigdata.search.ConfigurableAnalyzerFactory</code>)
<pre>
c.b.s.C.analyzer._.like=eng
c.b.s.C.analyzer.por.analyzerClass=org.apache.lucene.analysis.br.BrazilianAnalyzer
c.b.s.C.analyzer.pt.like=por
c.b.s.C.analyzer.zho.analyzerClass=org.apache.lucene.analysis.cn.ChineseAnalyzer
c.b.s.C.analyzer.chi.like=zho
c.b.s.C.analyzer.zh.like=zho
c.b.s.C.analyzer.jpn.analyzerClass=org.apache.lucene.analysis.cjk.CJKAnalyzer
c.b.s.C.analyzer.ja.like=jpn
c.b.s.C.analyzer.kor.like=jpn
c.b.s.C.analyzer.ko.like=kor
c.b.s.C.analyzer.ces.analyzerClass=org.apache.lucene.analysis.cz.CzechAnalyzer
c.b.s.C.analyzer.cze.like=ces
c.b.s.C.analyzer.cs.like=ces
c.b.s.C.analyzer.dut.analyzerClass=org.apache.lucene.analysis.nl.DutchAnalyzer
c.b.s.C.analyzer.nld.like=dut
c.b.s.C.analyzer.nl.like=dut
c.b.s.C.analyzer.deu.analyzerClass=org.apache.lucene.analysis.de.GermanAnalyzer
c.b.s.C.analyzer.ger.like=deu
c.b.s.C.analyzer.de.like=deu
c.b.s.C.analyzer.gre.analyzerClass=org.apache.lucene.analysis.el.GreekAnalyzer
c.b.s.C.analyzer.ell.like=gre
c.b.s.C.analyzer.el.like=gre
c.b.s.C.analyzer.rus.analyzerClass=org.apache.lucene.analysis.ru.RussianAnalyzer
c.b.s.C.analyzer.ru.like=rus
c.b.s.C.analyzer.tha.analyzerClass=org.apache.lucene.analysis.th.ThaiAnalyzer
c.b.s.C.analyzer.th.like=tha
c.b.s.C.analyzer.eng.analyzerClass=org.apache.lucene.analysis.standard.StandardAnalyzer
c.b.s.C.analyzer.en.like=eng
</pre>
    	 * 
    	 * 
    	 */
        String NATURAL_LANGUAGE_SUPPORT = ConfigurableAnalyzerFactory.class.getName() + ".naturalLanguageSupport";
        /**
         * This is the prefix to all properties configuring the individual analyzers.
         */
        String ANALYZER = ConfigurableAnalyzerFactory.class.getName() + ".analyzer.";

        String DEFAULT_NATURAL_LANGUAGE_SUPPORT = "false";
    }
    /**
     * Options understood by analyzers created by {@link ConfigurableAnalyzerFactory}.
     * These options are appended to the RFC 4647 language range
     */
    public interface AnalyzerOptions {
    	/**
    	 * If specified this is the fully qualified name of a subclass of {@link Analyzer}
    	 * that has appropriate constructors.
    	 * This is set implicitly if some of the options below are selected (for example {@link #PATTERN}).
    	 * For each configured language range, if it is not set, either explicitly or implicitly, then 
    	 * {@link #LIKE}  must be specified.
    	 */
        String ANALYZER_CLASS = "analyzerClass";
        
        /**
         * The value of this property is a language range, for which
         * an analyzer is defined. 
         * Treat this language range in the same way as the specified 
         * language range.
         * 
         * {@link #LIKE} loops are not permitted.
         * 
         * If this is option is specified for a language range,
         * then no other option is permitted.
         */
        String LIKE = "like";
        
        /**
         * The value of this property is one of:
         * <dl>
         * <dt>{@link #STOPWORDS_VALUE_NONE}</dt>
         * <dd>This analyzer is used without stop words.</dd>
         * <dt>{@link #STOPWORDS_VALUE_DEFAULT}</dt>
         * <dd>Use the default setting for stopwords for this analyzer. It is an error
         * to set this value on some analyzers such as {@link SimpleAnalyzer} that do not supprt stop words.
         * </dd>
         * <dt>A fully qualified class name</dt>
         * <dd>... of a subclass of {@link Analyzer} which
         * has a static method <code>getDefaultStopSet()</code>, in which case, the returned set of stop words is used.
         * </dd>
         * </dl>
         * If the {@link #ANALYZER_CLASS} does not support stop words then any value other than {@link #STOPWORDS_VALUE_NONE} is an error.
         * If the {@link #ANALYZER_CLASS} does support stop words then the default value is {@link #STOPWORDS_VALUE_DEFAULT}
         */
        String STOPWORDS = "stopwords";
        
        String STOPWORDS_VALUE_DEFAULT = "default";
        
        String STOPWORDS_VALUE_NONE = "none";
        /**
         * The value of the pattern parameter to
         * {@link PatternAnalyzer#PatternAnalyzer(Version, Pattern, boolean, Set)} 
         * (Note the {@link Pattern#UNICODE_CHARACTER_CLASS} flag is enabled).
         * It is an error if a different analyzer class is specified.
         */
        String PATTERN = "pattern";
        /**
         * The value of the wordBoundary parameter to
         * {@link TermCompletionAnalyzer#TermCompletionAnalyzer(Pattern, Pattern, Pattern, boolean)} 
         * (Note the {@link Pattern#UNICODE_CHARACTER_CLASS} flag is enabled).
         * It is an error if a different analyzer class is specified.
         */
        String WORD_BOUNDARY = "wordBoundary";
        /**
         * The value of the subWordBoundary parameter to
         * {@link TermCompletionAnalyzer#TermCompletionAnalyzer(Pattern, Pattern, Pattern, boolean)} 
         * (Note the {@link Pattern#UNICODE_CHARACTER_CLASS} flag is enabled).
         * It is an error if a different analyzer class is specified.
         */
        String SUB_WORD_BOUNDARY = "subWordBoundary";
        /**
         * The value of the softHyphens parameter to
         * {@link TermCompletionAnalyzer#TermCompletionAnalyzer(Pattern, Pattern, Pattern, boolean)} 
         * (Note the {@link Pattern#UNICODE_CHARACTER_CLASS} flag is enabled).
         * It is an error if a different analyzer class is specified.
         */
        String SOFT_HYPHENS = "softHyphens";
        /**
         * The value of the alwaysRemoveSoftHypens parameter to
         * {@link TermCompletionAnalyzer#TermCompletionAnalyzer(Pattern, Pattern, Pattern, boolean)} 
         * (Note the {@link Pattern#UNICODE_CHARACTER_CLASS} flag is enabled).
         * It is an error if a different analyzer class is specified.
         */
        String ALWAYS_REMOVE_SOFT_HYPHENS = "alwaysRemoveSoftHyphens";
        
        boolean DEFAULT_ALWAYS_REMOVE_SOFT_HYPHENS = false;

        /**
         * The default sub-word boundary is a pattern that never matches,
         * i.e. there are no sub-word boundaries.
         */
		Pattern DEFAULT_SUB_WORD_BOUNDARY = Pattern.compile("(?!)");
    	
    }

    /**
     * Initialization is a little tricky, because on the very first
     * call to the constructor with a new namespace or a new journal
     * the fullTextIndex is not ready for use.
     * Therefore we delegate to an unconfigured object
     * which on the first call to {@link NeedsConfiguringAnalyzerFactory#getAnalyzer(String, boolean)}
     * does the configuration and replaces itself here with a
     * {@link ConfiguredAnalyzerFactory}
     */
    IAnalyzerFactory delegate;

    /**
     * Builds a new ConfigurableAnalyzerFactory.
     * @param fullTextIndex
     */
    public ConfigurableAnalyzerFactory(final FullTextIndex<?> fullTextIndex) {
    	delegate = new NeedsConfiguringAnalyzerFactory(this, fullTextIndex);
    }


	static int loggerIdCounter = 0;
	@Override
	public Analyzer getAnalyzer(final String languageCode, boolean filterStopwords) {

		final Analyzer unlogged = delegate.getAnalyzer(languageCode, filterStopwords);
		if (log.isDebugEnabled()) {
			return new Analyzer() {
				@Override
				public TokenStream tokenStream(final String fieldName, final Reader reader) {
					final int id = loggerIdCounter++;
					final String term = TermCompletionAnalyzer.getStringReaderContents((StringReader)reader);
					log.debug(id + " " + languageCode +" **"+term+"**");
					return new TokenFilter(unlogged.tokenStream(fieldName, reader)){
						
						TermAttribute attr = addAttribute(TermAttribute.class);
						@Override
						public boolean incrementToken() throws IOException {
							if (input.incrementToken()) {
								log.debug(id + " |"+attr.term()+"|");
								return true;
							}
							return false;
						}};
				}
			};
		} else {
			return unlogged;
		}
		
	}

}
