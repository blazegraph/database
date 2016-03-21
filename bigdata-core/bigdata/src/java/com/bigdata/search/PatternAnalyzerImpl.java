package com.bigdata.search;

import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * Helper class created for Lucene 5.1.0 migration.
 * 
 * @author beebs
 *
 */
class PatternAnalyzerImpl extends Analyzer {
	
	private Pattern pattern = null;
	private boolean useStopWords = false;
	private CharArraySet stopWordList = null;

	@SuppressWarnings("unused")
	public PatternAnalyzerImpl(final Pattern pattern, final CharArraySet stopWordList) {
		super();
		this.pattern = pattern;
		this.stopWordList = stopWordList;
	}
	
	@SuppressWarnings("unused")
	public PatternAnalyzerImpl(final String range, final CharArraySet stopWordList,
			boolean useStopWords) {
		this.useStopWords = useStopWords;
		this.stopWordList = stopWordList;
	}
	
	@Override
	protected TokenStreamComponents createComponents(final String field) {
		//Use default grouping
		final Tokenizer tokenizer = new PatternTokenizer(pattern,-1);
		final TokenStream filter = new LowerCaseFilter(tokenizer);
		return new TokenStreamComponents(tokenizer, filter);
	}
}