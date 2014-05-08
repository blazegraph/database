/**

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
 * Created on May 6, 2014 by Jeremy J. Carroll, Syapse Inc.
 */
package com.bigdata.search;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;

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
 * <li>{@link Set} (of strings, the stop words)
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
 * 
 * If there are no such properties at all then the property {@link Options#INCLUDE_DEFAULTS} is set to true,
 * and the behavior of this class is the same as the legacy {@link DefaultAnalyzerFactory}.
 * <p>
 * Other properties, from {@link AnalyzerOptions} start with
 * <code>c.b.s.C.analyzer.<em>language-range</em></code> where <code><em>language-range</em></code> conforms
 * with the extended language range construct from RFC 4647, section 2.2. These are used to specify 
 * an analyzer for the given language range.
 * <p>
 * If no analyzer is specified for the language range <code>*</code> then the {@link StandardAnalyzer} is used.
 * <p>
 * Given any specific language, then the analyzer matching the longest configured language range, 
 * measured in number of subtags is used {@link #getAnalyzer(String, boolean)} 
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
 * <dt>{@link EmptyAnalyzer}</dt>
 * <dd>This suppresses the functionality, by treating every expression as a stop word.</dd>
 * </dl>
 * there are in addition the language specific analyzers that are included
 * by using the option {@link Options#INCLUDE_DEFAULTS}
 * 
 * 
 * @author jeremycarroll
 *
 */
public class ConfigurableAnalyzerFactory implements IAnalyzerFactory {
	final private static transient Logger log = Logger.getLogger(ConfigurableAnalyzerFactory.class);

	static class LanguageRange implements Comparable<LanguageRange> {
		
		private final String range[];
		private final String full;

		public LanguageRange(String range) {
			this.range = range.split("-");
			full = range;
		}

		@Override
		public int compareTo(LanguageRange o) {
			if (equals(o)) {
				return 0;
			}
			int diff = o.range.length - range.length;
			if (diff != 0) {
				// longest first
				return diff;
			}
			if (range.length == 1) {
				// * last
				if (range[0].equals("*")) {
					return 1;
				} 
				if (o.range[0].equals("*")) {
					return -1;
				}
			}
			// alphabetically
			for (int i=0; i<range.length; i++) {
				diff = range[i].compareTo(o.range[i]);
				if (diff != 0) {
					return diff;
				}
			}
			throw new RuntimeException("Impossible - supposedly");
		}
		
		@Override
		public boolean equals(Object o) {
			return (o instanceof LanguageRange) && ((LanguageRange)o).full.equals(full);
		}
		@Override 
		public int hashCode() {
			return full.hashCode();
		}
		
		public boolean extendedFilterMatch(String langTag) {
			return extendedFilterMatch(langTag.toLowerCase(Locale.ROOT).split("-"));
		}

		// See RFC 4647, 3.3.2
		public boolean extendedFilterMatch(String[] language) {
			// RFC 4647 step 2
			if (!matchSubTag(language[0], range[0])) {
				return false;
			}
			int rPos = 1;
			int lPos = 1;
			// variant step - for private use flags
			if (language[0].equals("x") && range[0].equals("*")) {
				lPos = 0;
			}
			// RFC 4647 step 3
			while (rPos < range.length) {
				// step 3A 
				if (range[rPos].equals("*")) {
					rPos ++;
					continue;
				}
				// step 3B
				if (lPos >= language.length) {
					return false;
				}
				// step 3C
				if (matchSubTag(language[lPos], range[rPos])) {
					lPos++;
					rPos++;
					continue;
				}
				if (language[lPos].length()==1) {
					return false;
				}
				lPos++;
			}
			// RFC 4647 step 4
			return true;
		}

		// RFC 4647, 3.3.2, step 1
		private boolean matchSubTag(String langSubTag, String rangeSubTag) {
			return langSubTag.equals(rangeSubTag) || "*".equals(rangeSubTag);
		}

	}
	/**
     * Options understood by the {@link ConfigurableAnalyzerFactory}.
     */
    public interface Options {
    	/**
    	 * By setting this option to true, then the behavior of the legacy {@link DefaultAnalyzerFactory}
    	 * is added, and may be overridden by the settings of the user.
    	 * Specifically the following properties are loaded, prior to loading the
    	 * user's specification (with <code>c.b.s.C</code> expanding to 
    	 * <code>com.bigdata.search.ConfigurableAnalyzerFactory</code>)
<pre>
c.b.s.C.analyzer.*.like=eng
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
        String INCLUDE_DEFAULTS = ConfigurableAnalyzerFactory.class.getName() + ".includeDefaults";
        /**
         * This is the prefix to all properties configuring the individual analyzers.
         */
        String ANALYZER = ConfigurableAnalyzerFactory.class.getName() + ".analyzer.";
/**
 * If there is no configuration at all, then the defaults are included,
 * but any configuration at all totally replaces the defaults, unless 
 * {@link #INCLUDE_DEFAULTS}
 * is explicitly set to true.
 */
        String DEFAULT_INCLUDE_DEFAULTS = "false";
    }
    /**
     * Options understood by analyzers created by {@link ConfigurableAnalyzerFactory}.
     * These options are appended to the RFC 4647 language range
     */
    public interface AnalyzerOptions {
    	/**
    	 * If specified this is the fully qualified name of a subclass of {@link Analyzer}
    	 * that has appropriate constructors.
    	 * Either this or {@link #LIKE} or {@link #PATTERN} must be specified for each language range.
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
         * If this property is present then the analyzer being used is a
         * {@link PatternAnalyzer} and the value is the pattern to use.
         * (Note the {@link Pattern#UNICODE_CHARACTER_CLASS} flag is enabled).
         * It is an error if a different analyzer class is specified.
         */
        String PATTERN = ".pattern";
    	
    }

	private static final String DEFAULT_PROPERTIES =  
			"com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.*.like=eng\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.por.analyzerClass=org.apache.lucene.analysis.br.BrazilianAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.pt.like=por\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.zho.analyzerClass=org.apache.lucene.analysis.cn.ChineseAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.chi.like=zho\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.zh.like=zho\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.jpn.analyzerClass=org.apache.lucene.analysis.cjk.CJKAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.ja.like=jpn\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.kor.like=jpn\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.ko.like=kor\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.ces.analyzerClass=org.apache.lucene.analysis.cz.CzechAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.cze.like=ces\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.cs.like=ces\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.dut.analyzerClass=org.apache.lucene.analysis.nl.DutchAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.nld.like=dut\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.nl.like=dut\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.deu.analyzerClass=org.apache.lucene.analysis.de.GermanAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.ger.like=deu\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.de.like=deu\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.gre.analyzerClass=org.apache.lucene.analysis.el.GreekAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.ell.like=gre\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.el.like=gre\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.rus.analyzerClass=org.apache.lucene.analysis.ru.RussianAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.ru.like=rus\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.tha.analyzerClass=org.apache.lucene.analysis.th.ThaiAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.th.like=tha\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.eng.analyzerClass=org.apache.lucene.analysis.standard.StandardAnalyzer\n" +
		    "com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.en.like=eng\n";

	private static class AnalyzerPair implements Comparable<AnalyzerPair>{
		private final LanguageRange range;
		private final Analyzer withStopWords;
		private final Analyzer withoutStopWords;
		
    	AnalyzerPair(String range, Analyzer withStopWords, Analyzer withOutStopWords) {
    		this.range = new LanguageRange(range);
    		this.withStopWords = withStopWords;
    		this.withoutStopWords = withOutStopWords;
    	}
    	
    	AnalyzerPair(String range, AnalyzerPair copyMe) {
    		this.range = new LanguageRange(range);
    		this.withStopWords = copyMe.withStopWords;
    		this.withoutStopWords = copyMe.withoutStopWords;
    		
    	}

		public Analyzer getAnalyzer(boolean filterStopwords) {
			return filterStopwords ? withStopWords : withoutStopWords;
		}
		@Override
		public String toString() {
			return range.full + "=(" + withStopWords.getClass().getSimpleName() +")";
		}
		
		
    	AnalyzerPair(String range, Constructor<? extends Analyzer> cons, Object ... params) throws Exception {
    		this(range, cons.newInstance(params), cons.newInstance(useEmptyStopWordSet(params)));
    	}
    	AnalyzerPair(String range, Analyzer stopWordsNotSupported) {
    		this(range, stopWordsNotSupported, stopWordsNotSupported);    		
    	}
		private static Object[] useEmptyStopWordSet(Object[] params) {
			Object rslt[] = new Object[params.length];
			for (int i=0; i<params.length; i++) {
				if (params[i] instanceof Set) {
					rslt[i] = Collections.EMPTY_SET;
				} else {
					rslt[i] = params[i];
				}
			}
			return rslt;
		}
		@Override
		public int compareTo(AnalyzerPair o) {
			return range.compareTo(o.range);
		}

		public boolean extendedFilterMatch(String[] language) {
			return range.extendedFilterMatch(language);
		}
	}
	

	private static class VersionSetAnalyzerPair extends AnalyzerPair {
		public VersionSetAnalyzerPair(ConfigOptionsToAnalyzer lro,
				Class<? extends Analyzer> cls) throws Exception {
			super(lro.languageRange, getConstructor(cls, Version.class, Set.class), Version.LUCENE_CURRENT, lro.getStopWords());
		}
	}
	
	private static class VersionAnalyzerPair extends AnalyzerPair {

		public VersionAnalyzerPair(String range, Class<? extends Analyzer> cls) throws Exception {
			super(range, getConstructor(cls, Version.class).newInstance(Version.LUCENE_CURRENT));
		}
	}
	
	
    private static class PatternAnalyzerPair extends AnalyzerPair {

		public PatternAnalyzerPair(ConfigOptionsToAnalyzer lro, String pattern) throws Exception {
			super(lro.languageRange, getConstructor(PatternAnalyzer.class,Version.class,Pattern.class,Boolean.TYPE,Set.class), 
				Version.LUCENE_CURRENT, 
				Pattern.compile(pattern, Pattern.UNICODE_CHARACTER_CLASS),
				true,
				lro.getStopWords());
		}
	}


	/**
	 * This class is initialized with the config options, using the {@link #setProperty(String, String)}
	 * method, for a particular language range and works out which pair of {@link Analyzer}s
	 * to use for that language range.
	 * @author jeremycarroll
	 *
	 */
    private static class ConfigOptionsToAnalyzer {
    	
    	String like;
    	String className;
    	String stopwords;
    	String pattern;
    	final String languageRange;
    	AnalyzerPair result;

		public ConfigOptionsToAnalyzer(String languageRange) {
			this.languageRange = languageRange;
		}

		/**
		 * This is called only when we have already identified that
		 * the class does support stopwords.
		 * @return
		 */
		public Set<?> getStopWords() {
			
			if (AnalyzerOptions.STOPWORDS_VALUE_NONE.equals(stopwords)) 
				return Collections.EMPTY_SET;
			
			if (useDefaultStopWords()) {
				return getStopWordsForClass(className);
			}
			
			return getStopWordsForClass(stopwords);
		}

		protected Set<?> getStopWordsForClass(String clazzName) {
			Class<? extends Analyzer> analyzerClass = getAnalyzerClass(clazzName);
			try {
				return (Set<?>) analyzerClass.getMethod("getDefaultStopSet").invoke(null);
			} catch (Exception e) {
				if (StandardAnalyzer.class.equals(analyzerClass)) {
					return StandardAnalyzer.STOP_WORDS_SET;
				}
				if (StopAnalyzer.class.equals(analyzerClass)) {
					return StopAnalyzer.ENGLISH_STOP_WORDS_SET;
				}
				throw new RuntimeException("Failed to find stop words from " + clazzName + " for language range "+languageRange);
			}
		}

		protected boolean useDefaultStopWords() {
			return stopwords == null || AnalyzerOptions.STOPWORDS_VALUE_DEFAULT.equals(stopwords);
		}

		public boolean setProperty(String shortProperty, String value) {
			if (shortProperty.equals(AnalyzerOptions.LIKE) ) {
				like = value;
			} else if (shortProperty.equals(AnalyzerOptions.ANALYZER_CLASS) ) {
				className = value;
			} else if (shortProperty.equals(AnalyzerOptions.STOPWORDS) ) {
				stopwords = value;
			} else if (shortProperty.equals(AnalyzerOptions.PATTERN) ) {
				pattern = value;
			} else {
			   return false;
			}
			return true;
		}

		public void validate() {
			if (pattern != null ) {
				if ( className != null && className != PatternAnalyzer.class.getName()) {
					throw new RuntimeException("Bad Option: Language range "+languageRange + " with pattern propety for class "+ className);
				}
				className = PatternAnalyzer.class.getName();
			}
			if (PatternAnalyzer.class.getName().equals(className) && pattern == null ) {
				throw new RuntimeException("Bad Option: Language range "+languageRange + " must specify pattern for PatternAnalyzer.");
			}
			if ( (like != null) == (className != null) ) {
				throw new RuntimeException("Bad Option: Language range "+languageRange + " must specify exactly one of implementation class or like.");
			}
			if (stopwords != null && like != null) {
				throw new RuntimeException("Bad Option: Language range "+languageRange + " must not specify stopwords with like.");
			}
			
		}
		
		private AnalyzerPair construct() throws Exception {
			if (className == null) {
				return null;
			}
			if (pattern != null) {
				return new PatternAnalyzerPair(this, pattern);
						
			} 
			final Class<? extends Analyzer> cls = getAnalyzerClass();
            
            if (hasConstructor(cls, Version.class, Set.class)) {

            	// RussianAnalyzer is missing any way to access stop words.
            	if (RussianAnalyzer.class.equals(cls) && useDefaultStopWords()) {
            		return new AnalyzerPair(languageRange, new RussianAnalyzer(Version.LUCENE_CURRENT), new RussianAnalyzer(Version.LUCENE_CURRENT, Collections.EMPTY_SET));
            	}
            	return new VersionSetAnalyzerPair(this, cls);
            }
            
            if (stopwords != null && !stopwords.equals(AnalyzerOptions.STOPWORDS_VALUE_NONE)) {
            	throw new RuntimeException("Bad option: language range: " + languageRange + " stopwords are not supported by " + className);
            }
            if (hasConstructor(cls, Version.class)) {
            	return new VersionAnalyzerPair(languageRange, cls);
            }
            
            if (hasConstructor(cls)) {
            	return new AnalyzerPair(languageRange, cls.newInstance());
            }
            throw new RuntimeException("Bad option: cannot find constructor for class " + className + " for language range " + languageRange);
		}

		protected Class<? extends Analyzer> getAnalyzerClass() {
			return getAnalyzerClass(className);
		}

		@SuppressWarnings("unchecked")
		protected Class<? extends Analyzer> getAnalyzerClass(String className2) {
			final Class<? extends Analyzer> cls;
			try {
                cls = (Class<? extends Analyzer>) Class.forName(className2);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Bad option: cannot find class " + className2 + " for language range " + languageRange, e);
            }
			return cls;
		}

		void setAnalyzerPair(AnalyzerPair ap) {
			result = ap;
		}

		AnalyzerPair followLikesToAnalyzerPair(int depth, int max,
				Map<String, ConfigOptionsToAnalyzer> analyzers) {
			if (result == null) {
				if (depth == max) {
					throw new RuntimeException("Bad configuration: - 'like' loop for language range " + languageRange);
				}
				ConfigOptionsToAnalyzer next = analyzers.get(like);
				if (next == null) {
					throw new RuntimeException("Bad option: - 'like' not found for language range " + languageRange+ " (not found: '"+ like +"')");	
				}
				result = new AnalyzerPair(languageRange, next.followLikesToAnalyzerPair(depth+1, max, analyzers));
			}
			return result;
		}

	}
    
    private final AnalyzerPair config[];
    
    private final Map<String, AnalyzerPair> langTag2AnalyzerPair = new ConcurrentHashMap<String, AnalyzerPair>();
    
    /**
     * While it would be very unusual to have more than 500 different language tags in a store
     * it is possible - we use a max size to prevent a memory explosion, and a naive caching
     * strategy so the code will still work on the {@link #MAX_LANG_CACHE_SIZE}+1 th entry.
     */
    private static final int MAX_LANG_CACHE_SIZE = 500;
    		
    private String defaultLanguage;
    private final FullTextIndex<?> fullTextIndex;
    
    
    public ConfigurableAnalyzerFactory(final FullTextIndex<?> fullTextIndex) {
    	// despite our name, we actually make all the analyzers now, and getAnalyzer method is merely a lookup.

        if (fullTextIndex == null)
            throw new IllegalArgumentException();
        
        this.fullTextIndex = fullTextIndex;
        
        final Properties properties = initProperties();
        
        final Map<String, ConfigOptionsToAnalyzer> analyzers = new HashMap<String, ConfigOptionsToAnalyzer>();
        
        properties2analyzers(properties, analyzers);
        
        if (!analyzers.containsKey("*")) {
        	throw new RuntimeException("Bad config: must specify behavior on language range '*'");
        }
        
        for (ConfigOptionsToAnalyzer a: analyzers.values()) {
        	a.validate();
        }

        try {
			for (ConfigOptionsToAnalyzer a: analyzers.values()) {
				a.setAnalyzerPair(a.construct());
			}
		} catch (Exception e) {
			throw new RuntimeException("Cannot construct ConfigurableAnalyzerFactory", e);
		}
        int sz = analyzers.size();
		for (ConfigOptionsToAnalyzer a: analyzers.values()) {
			a.followLikesToAnalyzerPair(0, sz, analyzers);
		}
		
		config = new AnalyzerPair[sz];
		int i = 0;
		for (ConfigOptionsToAnalyzer a: analyzers.values()) {
			config[i++] = a.result;
		}
		Arrays.sort(config);
		if (log.isInfoEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("Installed text Analyzer's: ");
			for (AnalyzerPair ap: config) {
				sb.append(ap.toString());
				sb.append(", ");
			}
			log.info(sb.toString());
		}
    }

	private String getDefaultLanguage(final FullTextIndex<?> fullTextIndex) {
		
		final IKeyBuilder keyBuilder = fullTextIndex.getKeyBuilder();


		if (keyBuilder.isUnicodeSupported()) {

			// The configured local for the database.
			final Locale locale = ((KeyBuilder) keyBuilder)
					.getSortKeyGenerator().getLocale();

			// The analyzer for that locale.
			return locale.getLanguage();
			
		} else {
			// Rule, Britannia!
			return "en"; 
			
		}
	}
	private String getDefaultLanguage() {
		if (defaultLanguage == null) {
            defaultLanguage = getDefaultLanguage(fullTextIndex);
		}
		return defaultLanguage;
	}

	private static boolean hasConstructor(Class<? extends Analyzer> cls, Class<?> ... parameterTypes) {
		return getConstructor(cls, parameterTypes) != null;
	}

	protected static Constructor<? extends Analyzer> getConstructor(Class<? extends Analyzer> cls,
			Class<?>... parameterTypes) {
		try {
			return cls.getConstructor(parameterTypes);
		} catch (NoSuchMethodException | SecurityException e) {
			return null;
		}
	}

	private void properties2analyzers(Properties props, Map<String, ConfigOptionsToAnalyzer> analyzers) {
		
		Enumeration<?> en = props.propertyNames();
		while (en.hasMoreElements()) {
			
			String prop = (String)en.nextElement();
			if (prop.equals(Options.INCLUDE_DEFAULTS)) continue;
			if (prop.startsWith(Options.ANALYZER)) {
				String languageRangeAndProperty[] = prop.substring(Options.ANALYZER.length()).split("[.]");
				if (languageRangeAndProperty.length == 2) {

					String languageRange = languageRangeAndProperty[0].toLowerCase(Locale.US);  // Turkish "I" could create a problem
					String shortProperty = languageRangeAndProperty[1];
					String value =  props.getProperty(prop);
					log.info("Setting language range: " + languageRange + "/" + shortProperty + " = " + value);
					ConfigOptionsToAnalyzer cons = analyzers.get(languageRange);
					if (cons == null) {
						cons = new ConfigOptionsToAnalyzer(languageRange);
						analyzers.put(languageRange, cons);
					}
					if (cons.setProperty(shortProperty, value)) {
						continue;
					}
				}
			} 
			
			log.warn("Failed to process configuration property: " + prop);
		}
		
	}

	protected Properties initProperties() {
		final Properties parentProperties = fullTextIndex.getProperties();
        Properties myProps;
        if (Boolean.getBoolean(parentProperties.getProperty(Options.INCLUDE_DEFAULTS, Options.DEFAULT_INCLUDE_DEFAULTS))) {
        	myProps = defaultProperties();
        } else {
        	myProps = new Properties();
        }
        
        copyRelevantProperties(fullTextIndex.getProperties(), myProps);
        
        if (myProps.isEmpty()) {
        	return defaultProperties();
        } else {
		    return myProps;
        }
	}

	protected Properties defaultProperties() {
		Properties rslt = new Properties();
		try {
			rslt.load(new StringReader(DEFAULT_PROPERTIES));
		} catch (IOException e) {
			throw new RuntimeException("Impossible - well clearly not!", e);
		}
		return rslt;
	}
    
    private void copyRelevantProperties(Properties from, Properties to) {
		Enumeration<?> en = from.propertyNames();
		while (en.hasMoreElements()) {
			String prop = (String)en.nextElement();
			if (prop.startsWith(ConfigurableAnalyzerFactory.class.getName())) {
				to.setProperty(prop, from.getProperty(prop));
			}
		}
	}

	@Override
	public Analyzer getAnalyzer(String languageCode, boolean filterStopwords) {
		
		if (languageCode == null || languageCode.equals("")) {
			
			languageCode = getDefaultLanguage();
		}
		
		AnalyzerPair pair = langTag2AnalyzerPair.get(languageCode);
		
		if (pair == null) {
			pair = lookupPair(languageCode);
			
			// naive cache - clear everything if cache is full
			if (langTag2AnalyzerPair.size() == MAX_LANG_CACHE_SIZE) {
				langTag2AnalyzerPair.clear();
			}
			// there is a race condition below, but we don't care who wins.
			langTag2AnalyzerPair.put(languageCode, pair);
		}
		
		return pair.getAnalyzer(filterStopwords);
		
	}

	private AnalyzerPair lookupPair(String languageCode) {
		String language[] = languageCode.split("-");
		for (AnalyzerPair p: config) {
			if (p.extendedFilterMatch(language)) {
				return p;
			}
		}
		throw new RuntimeException("Impossible - supposedly - did not match '*'");
	}
}
