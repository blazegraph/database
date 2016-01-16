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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;

import com.bigdata.search.ConfigurableAnalyzerFactory.AnalyzerOptions;
/**
 * This comment describes the implementation of {@link ConfiguredAnalyzerFactory}.
 * The only method in the interface is {@link ConfiguredAnalyzerFactory#getAnalyzer(String, boolean)},
 * a map is used from language tag to {@link AnalyzerPair}, where the pair contains
 * an {@link Analyzer} both with and without stopwords configured (some times these two analyzers are identical,
 * if, for example, stop words are not supported or not required).
 * <p>
 * If there is no entry for the language tag in the map {@link ConfiguredAnalyzerFactory#langTag2AnalyzerPair},
 * then one is created, by walking down the array {@link ConfiguredAnalyzerFactory#config} of AnalyzerPairs
 * until a matching one is found.
 * @author jeremycarroll
 *
 */
class ConfiguredAnalyzerFactory implements IAnalyzerFactory {


	/**
	 * These provide a mapping from a language range to a pair of Analyzers
	 * and sort with the best-match (i.e. longest match) first.
	 * @author jeremycarroll
	 *
	 */
	protected static class AnalyzerPair implements Comparable<AnalyzerPair>{
		final LanguageRange range;
		private final Analyzer withStopWords;
		private final Analyzer withoutStopWords;
		
		public Analyzer getAnalyzer(boolean filterStopwords) {
			return filterStopwords ? withStopWords : withoutStopWords;
		}
		
		public boolean extendedFilterMatch(String[] language) {
			return range.extendedFilterMatch(language);
		}
		
    	AnalyzerPair(String range, Analyzer withStopWords, Analyzer withOutStopWords) {
    		this.range = new LanguageRange(range);
    		this.withStopWords = withStopWords;
    		this.withoutStopWords = withOutStopWords;
    	}
    	
    	/**
    	 * This clone constructor implements {@link AnalyzerOptions#LIKE}.
    	 * @param range
    	 * @param copyMe
    	 */
    	AnalyzerPair(String range, AnalyzerPair copyMe) {
    		this(range, copyMe.withStopWords, copyMe.withoutStopWords);
    	}

		@Override
		public String toString() {
			return range.full + "=(" + withStopWords.getClass().getSimpleName() +")";
		}
		
		@Override
		public int compareTo(AnalyzerPair o) {
			return range.compareTo(o.range);
		}
	}
	

    private final AnalyzerPair config[];
    
    /**
     * This caches the result of looking up a lang tag in the
     * config of language ranges.
     */
    private final Map<String, AnalyzerPair> langTag2AnalyzerPair = new ConcurrentHashMap<String, AnalyzerPair>();;
    
    /**
     * While it would be very unusual to have more than 500 different language tags in a store
     * it is possible - we use a max size to prevent a memory explosion, and a naive caching
     * strategy so the code will still work on the {@link #MAX_LANG_CACHE_SIZE}+1 th entry.
     */
    private static final int MAX_LANG_CACHE_SIZE = 500;

    		
    private final String defaultLanguage;
    /**
     * Builds a new ConfigurableAnalyzerFactory.
     * @param fullTextIndex
     */
    public ConfiguredAnalyzerFactory(AnalyzerPair config[],  String defaultLanguage) {
    	this.config = config;
    	this.defaultLanguage = defaultLanguage;
    }

	private String getDefaultLanguage() {
		return defaultLanguage;
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
