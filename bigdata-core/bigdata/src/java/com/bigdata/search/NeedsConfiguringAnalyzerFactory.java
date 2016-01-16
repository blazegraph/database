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
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.search.ConfigurableAnalyzerFactory.AnalyzerOptions;
import com.bigdata.search.ConfigurableAnalyzerFactory.Options;


/**
 * <p>
 * The bulk of the code in this class is invoked from {@link #init()} to set up the array of
 *  {@link ConfiguredAnalyzerFactory.AnalyzerPair}s. For example, all of the subclasses of {@link AnalyzerPair}s,
 *  are simply to call the appropriate constructor in the appropriate way: the difficulty is that many subclasses
 *  of {@link Analyzer} have constructors with different signatures, and our code needs to navigate each sort.
 * @author jeremycarroll
 *
 */
class NeedsConfiguringAnalyzerFactory implements IAnalyzerFactory {
	final private static transient Logger log = Logger.getLogger(NeedsConfiguringAnalyzerFactory.class);
	
	/**
	 * We create only one {@link ConfiguredAnalyzerFactory} per namespace
	 * and store it here. The UUID is stable and allows us to side-step lifecycle
	 * issues such as creation and destruction of namespaces, potentially with different properties.
	 * We use a WeakHashMap to ensure that after the destruction of a namespace we clean up.
	 * We have to synchronize this for thread safety.
	 */
    private static final Map<UUID, ConfiguredAnalyzerFactory> allConfigs = 
    		Collections.synchronizedMap(new WeakHashMap<UUID, ConfiguredAnalyzerFactory>());


	private static final String ALL_LUCENE_NATURAL_LANGUAGES =  
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

	private static final String LUCENE_STANDARD_ANALYZER = 
			"com.bigdata.search.ConfigurableAnalyzerFactory.analyzer.*.analyzerClass=org.apache.lucene.analysis.standard.StandardAnalyzer\n";
	
	static int loggerIdCounter = 0;

	/**
	 * This class and all its subclasses provide a variety of patterns
	 * for mapping from the various constructor patterns of subclasses
	 * of {@link Analyzer} to {@link ConfiguredAnalyzerFactory#AnalyzerPair}.
	 * @author jeremycarroll
	 *
	 */
	private static class AnalyzerPair extends ConfiguredAnalyzerFactory.AnalyzerPair {
		
    	AnalyzerPair(String range, Analyzer withStopWords, Analyzer withOutStopWords) {
    		super(range, withStopWords, withOutStopWords);
    	}
    	
    	/**
    	 * This clone constructor implements {@link AnalyzerOptions#LIKE}.
    	 * @param range
    	 * @param copyMe
    	 */
    	AnalyzerPair(String range, AnalyzerPair copyMe) {
    		super(range, copyMe);
    	}
		
    	/**
    	 * If we have a constructor, with arguments including a populated
    	 * stop word set, then we can use it to make both the withStopWords
    	 * analyzer, and the withoutStopWords analyzer.
    	 * @param range
    	 * @param cons A Constructor including a {@link java.util.Set} argument
    	 *  for the stop words.
    	 * @param params The arguments to pass to the constructor including a populated stopword set.
    	 * @throws Exception
    	 */
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

	}
	

	/**
	 * Used for Analyzer classes with a constructor with signature (Version, Set).
	 * @author jeremycarroll
	 *
	 */
	private static class VersionSetAnalyzerPair extends AnalyzerPair {
		public VersionSetAnalyzerPair(ConfigOptionsToAnalyzer lro,
				Class<? extends Analyzer> cls) throws Exception {
			super(lro.languageRange, getConstructor(cls, Version.class, Set.class), Version.LUCENE_CURRENT, lro.getStopWords());
		}
	}

	/**
	 * Used for Analyzer classes which do not support stopwords and have a constructor with signature (Version).
	 * @author jeremycarroll
	 *
	 */
	private static class VersionAnalyzerPair extends AnalyzerPair {
		public VersionAnalyzerPair(String range, Class<? extends Analyzer> cls) throws Exception {
			super(range, getConstructor(cls, Version.class).newInstance(Version.LUCENE_CURRENT));
		}
	}
	
	/**
	 * Special case code for {@link PatternAnalyzer}
	 * @author jeremycarroll
	 *
	 */
    private static class PatternAnalyzerPair extends AnalyzerPair {
		public PatternAnalyzerPair(ConfigOptionsToAnalyzer lro, Pattern pattern) throws Exception {
			super(lro.languageRange, getConstructor(PatternAnalyzer.class,Version.class,Pattern.class,Boolean.TYPE,Set.class), 
				Version.LUCENE_CURRENT, 
				pattern,
				true,
				lro.getStopWords());
		}
	}
    


	/**
	 * This class is initialized with the config options, using the {@link #setProperty(String, String)}
	 * method, for a particular language range and works out which pair of {@link Analyzer}s
	 * to use for that language range.
	 * <p>
	 * Instances of this class are only alive during the execution of 
	 * {@link NeedsConfiguringAnalyzerFactory#ConfigurableAnalyzerFactory(FullTextIndex)},
	 * the life-cycle is:
	 * <ol>
	 * <li>The relveant config properties are applied, and are used to populate the fields.
	 * <li>The fields are validated
	 * <li>An {@link AnalyzerPair} is constructed
	 * </ol>
	 * 
	 * @author jeremycarroll
	 *
	 */
    private static class ConfigOptionsToAnalyzer {
    	
    	String like;
    	String className;
    	String stopwords;
    	Pattern pattern;
    	final String languageRange;
    	AnalyzerPair result;
		Pattern wordBoundary;
		Pattern subWordBoundary;
		Pattern softHyphens;
		Boolean alwaysRemoveSoftHyphens;

		public ConfigOptionsToAnalyzer(String languageRange) {
			this.languageRange = languageRange;
		}

		/**
		 * This is called only when we have already identified that
		 * the class does support stopwords.
		 * @return
		 */
		public Set<?> getStopWords() {
			
			if (doNotUseStopWords()) 
				return Collections.EMPTY_SET;
			
			if (useDefaultStopWords()) {
				return getStopWordsForClass(className);
			}
			
			return getStopWordsForClass(stopwords);
		}

		boolean doNotUseStopWords() {
			return AnalyzerOptions.STOPWORDS_VALUE_NONE.equals(stopwords) || (stopwords == null && pattern != null);
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
			return ( stopwords == null && pattern == null ) || AnalyzerOptions.STOPWORDS_VALUE_DEFAULT.equals(stopwords);
		}

		/**
		 * The first step in the life-cycle, used to initialize the fields.
		 * @return true if the property was recognized.
		 */
		public boolean setProperty(String shortProperty, String value) {
			if (shortProperty.equals(AnalyzerOptions.LIKE) ) {
				like = value;
			} else if (shortProperty.equals(AnalyzerOptions.ANALYZER_CLASS) ) {
				className = value;
			} else if (shortProperty.equals(AnalyzerOptions.STOPWORDS) ) {
				stopwords = value;
			} else if (shortProperty.equals(AnalyzerOptions.PATTERN) ) {
				pattern = Pattern.compile(value,Pattern.UNICODE_CHARACTER_CLASS);
			} else if (shortProperty.equals(AnalyzerOptions.WORD_BOUNDARY) ) {
				wordBoundary = Pattern.compile(value,Pattern.UNICODE_CHARACTER_CLASS);
			} else if (shortProperty.equals(AnalyzerOptions.SUB_WORD_BOUNDARY) ) {
				subWordBoundary = Pattern.compile(value,Pattern.UNICODE_CHARACTER_CLASS);
			} else if (shortProperty.equals(AnalyzerOptions.SOFT_HYPHENS) ) {
				softHyphens = Pattern.compile(value,Pattern.UNICODE_CHARACTER_CLASS);
			} else if (shortProperty.equals(AnalyzerOptions.ALWAYS_REMOVE_SOFT_HYPHENS) ) {
				alwaysRemoveSoftHyphens = Boolean.valueOf(value);
			} else {
			   return false;
			}
			return true;
		}

		/**
		 * The second phase of the life-cycle, used for sanity checking.
		 */
		public void validate() {
			if (pattern != null ) {
				if ( className != null && className != PatternAnalyzer.class.getName()) {
					throw new RuntimeException("Bad Option: Language range "+languageRange + " with pattern propety for class "+ className);
				}
				className = PatternAnalyzer.class.getName();
			}
			if (this.wordBoundary != null  ) {
				if ( className != null && className != TermCompletionAnalyzer.class.getName()) {
					throw new RuntimeException("Bad Option: Language range "+languageRange + " with pattern propety for class "+ className);
				}
				className = TermCompletionAnalyzer.class.getName();
				
				if ( subWordBoundary == null ) {
					subWordBoundary = AnalyzerOptions.DEFAULT_SUB_WORD_BOUNDARY;
				}
				if ( alwaysRemoveSoftHyphens != null && softHyphens == null ) {
					throw new RuntimeException("Bad option: Language range "+languageRange + ": must specify softHypens when setting alwaysRemoveSoftHyphens");		
				}
				if (softHyphens != null && alwaysRemoveSoftHyphens == null) {
					alwaysRemoveSoftHyphens = AnalyzerOptions.DEFAULT_ALWAYS_REMOVE_SOFT_HYPHENS;
				}
				
			} else if ( subWordBoundary != null || softHyphens != null || alwaysRemoveSoftHyphens != null ||
					TermCompletionAnalyzer.class.getName().equals(className) ) {
				throw new RuntimeException("Bad option: Language range "+languageRange + ": must specify wordBoundary for TermCompletionAnalyzer");
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
		
		/**
		 * The third and final phase of the life-cyle used for identifying
		 * the AnalyzerPair.
		 */
		private AnalyzerPair construct() throws Exception {
			if (className == null) {
				return null;
			}
			if (pattern != null) {
				return new PatternAnalyzerPair(this, pattern);
			}
			if (softHyphens != null) {
				return new AnalyzerPair(
						languageRange,
						new TermCompletionAnalyzer(
								wordBoundary, 
								subWordBoundary, 
								softHyphens, 
								alwaysRemoveSoftHyphens));
			}
			if (wordBoundary != null) {
				return new AnalyzerPair(
						languageRange,
						new TermCompletionAnalyzer(
								wordBoundary, 
								subWordBoundary));
			}
			final Class<? extends Analyzer> cls = getAnalyzerClass();
            
            if (hasConstructor(cls, Version.class, Set.class)) {

            	// RussianAnalyzer is missing any way to access stop words.
            	if (RussianAnalyzer.class.equals(cls)) {
            		if (useDefaultStopWords()) {
            		    return new AnalyzerPair(languageRange, new RussianAnalyzer(Version.LUCENE_CURRENT), new RussianAnalyzer(Version.LUCENE_CURRENT, Collections.EMPTY_SET));
            		}
            		if (doNotUseStopWords()) {
            		    return new AnalyzerPair(languageRange,  new RussianAnalyzer(Version.LUCENE_CURRENT, Collections.EMPTY_SET));	
            		}
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

		/**
		 * Also part of the third phase of the life-cycle, following the {@link AnalyzerOptions#LIKE}
		 * properties.
		 * @param depth
		 * @param max
		 * @param analyzers
		 * @return
		 */
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
	}
    
    		
    private final FullTextIndex<?> fullTextIndex;

	private final ConfigurableAnalyzerFactory parent;
    
    
    NeedsConfiguringAnalyzerFactory(ConfigurableAnalyzerFactory parent, final FullTextIndex<?> fullTextIndex) {
        if (fullTextIndex == null)
            throw new IllegalArgumentException();
		this.fullTextIndex = fullTextIndex;
		this.parent = parent;
		
    }

	private ConfiguredAnalyzerFactory init() {
        
        UUID uuid = fullTextIndex.getIndex().getIndexMetadata().getIndexUUID();
        
        ConfiguredAnalyzerFactory configuration = allConfigs.get(uuid);
        
        if (configuration == null) {
        	
        	// First time for this namespace - we need to analyze the properties
        	// and construct a ConfiguredAnalyzerFactory
	        
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
			
			AnalyzerPair[] allPairs = new AnalyzerPair[sz];
			int i = 0;
			for (ConfigOptionsToAnalyzer a: analyzers.values()) {
				allPairs[i++] = a.result;
			}
			Arrays.sort(allPairs);
			if (log.isInfoEnabled()) {
				StringBuilder sb = new StringBuilder();
				sb.append("Installed text Analyzer's: ");
				for (AnalyzerPair ap: allPairs) {
					sb.append(ap.toString());
					sb.append(", ");
				}
				log.info(sb.toString());
			}
			configuration = new ConfiguredAnalyzerFactory(allPairs, getDefaultLanguage());
			allConfigs.put(uuid, configuration);
        }
        return configuration;
	}

	private String getDefaultLanguage() {
		
		final IKeyBuilder keyBuilder = fullTextIndex.getKeyBuilder();

		if (keyBuilder.isUnicodeSupported()) {

			// The configured locale for the database.
			final Locale locale = ((KeyBuilder) keyBuilder)
					.getSortKeyGenerator().getLocale();

			// The language for that locale.
			return locale.getLanguage();
			
		} else {
			// Rule, Britannia!
			return "en"; 
			
		}
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
			if (prop.equals(Options.NATURAL_LANGUAGE_SUPPORT)) continue;
			if (prop.startsWith(Options.ANALYZER)) {
				String languageRangeAndProperty[] = prop.substring(Options.ANALYZER.length()).replaceAll("_","*").split("[.]");
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
        if (Boolean.valueOf(parentProperties.getProperty(
        		Options.NATURAL_LANGUAGE_SUPPORT, 
        		Options.DEFAULT_NATURAL_LANGUAGE_SUPPORT))) {
        	
        	myProps = loadPropertyString(ALL_LUCENE_NATURAL_LANGUAGES);
        	
        } else  if (hasPropertiesForStarLanguageRange(parentProperties)){
        	
        	myProps = new Properties();
        	
        } else {
        	
        	myProps = loadPropertyString(LUCENE_STANDARD_ANALYZER);
        }
        
        copyRelevantProperties(fullTextIndex.getProperties(), myProps);
        return myProps;
	}

	Properties loadPropertyString(String props) {
		Properties rslt = new Properties();
		try {
			rslt.load(new StringReader(props));
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

    private boolean hasPropertiesForStarLanguageRange(Properties from) {
		Enumeration<?> en = from.propertyNames();
		while (en.hasMoreElements()) {
			String prop = (String)en.nextElement();
			if (prop.startsWith(Options.ANALYZER+"_.") 
					|| prop.startsWith(Options.ANALYZER+"*.")) {
				return true;
			}
		}
		return false;
	}
	@Override
	public Analyzer getAnalyzer(String languageCode, boolean filterStopwords) {
		// delayed initialization of parent
		IAnalyzerFactory realDelegate = init();
		parent.delegate = realDelegate;
		return realDelegate.getAnalyzer(languageCode, filterStopwords);
	}

}
