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
 * Created on May 8, 2014 by Jeremy J. Carroll, Syapse Inc.
 */
package com.bigdata.search;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;


/**
 * An analyzer intended for the term-completion use case; particularly
 * for technical vocabularies and concept schemes.
 * 
 * <p> 
 * This analyzer generates several index terms for each word in the input.
 * These are intended to match short sequences (e.g. three or more) characters
 * of user-input, to then give the user a drop-down list of matching terms.
 * <p>
 * This can be set up to address issues like matching <q>half-time</q> when the user types
 * <q>tim</q> or if the user types <q>halft</q> (treating the hyphen as a soft hyphen); or
 * to match <q>TermCompletionAnalyzer</q> when the user types <q>Ana</q>
 * <p>
 * In contrast, the Lucene Analyzers are mainly geared around the free text search use
 * case. 
 * <p>
 * The intended use cases will typical involve a prefix query of the form:
 * <pre>
 *    ?t bds:search "prefix*" .
 * </pre>
 * to find all literals in the selected graphs, which are indexed by a term starting in <q>prefix</q>,
 * so the problem this class addresses is finding the appropriate index terms to allow
 * matching, at sensible points, mid-way through words (such as at hyphens).
 * <p>
 * To get maximum effectiveness it maybe best to use private language subtags (see RFC 5647),
 * e.g. <code>"x-term"</code>
 * which are mapped to this class by {@link ConfigurableAnalyzerFactory} for
 * the data being loaded into the store, and linked to some very simple process
 * like {@link KeywordAnalyzer} for queries which are tagged with a different language tag
 * that is only used for <code>bds:search</code>, e.g. <code>"x-query"</code>.
 * The above prefix query then becomes:
 * <pre>
 *    ?t bds:search "prefix*"@x-query .
 * </pre>
 * 
 * 
 * 
 * @author jeremycarroll
 *
 */
public class TermCompletionAnalyzer extends Analyzer {
	
	private final Pattern wordBoundary;
	private final Pattern subWordBoundary;

	private final Pattern discard;
	private final boolean alwaysDiscard;

	/**
	 * Divide the input into words and short tokens
	 * as with {@link #TermCompletionAnalyzer(Pattern, Pattern)}.
	 * Each term is generated, and then an additional term
	 * is generated with softHypens (defined by the pattern),
	 * removed. If the alwaysRemoveSoftHypens flag is true,
	 * then the first term (before the removal) is suppressed.
	 * 
	 * @param wordBoundary      The definition of space (e.g. " ")
	 * @param subWordBoundary   Also index after matches to this (e.g. "-")
	 * @param softHyphens     Discard these characters from matches
	 * @param alwaysRemoveSoftHypens  If false the discard step is optional.
	 */
	public TermCompletionAnalyzer(Pattern wordBoundary, 
			Pattern subWordBoundary, 
			Pattern softHyphens,
			boolean alwaysRemoveSoftHypens) {
		this.wordBoundary = wordBoundary;
		this.subWordBoundary = subWordBoundary;
		if (softHyphens != null) {
			discard = softHyphens;
			alwaysDiscard = alwaysRemoveSoftHypens;
		} else {
			discard = Pattern.compile("(?!)"); // never matches
			alwaysDiscard = true;
		}
	}
	/**
	 * Divide the input into words, separated by the wordBoundary,
	 * and return a token for each whole word, and then 
	 * generate further tokens for each word by removing prefixes
	 * up to and including each successive match of
	 * subWordBoundary
	 * @param wordBoundary
	 * @param subWordBoundary
	 */
	public TermCompletionAnalyzer(Pattern wordBoundary, 
			Pattern subWordBoundary) {
		this(wordBoundary, subWordBoundary, null, true);
	}


	@Override
	public TokenStream tokenStream(String ignoredFieldName, Reader reader) {
		return new TermCompletionTokenStream((StringReader)reader);
	}

	/**
	 * This classes has three processes going on
	 * all driven from the {@link #increment()} method.
	 * 
	 * One process is that of iterating over the words in the input:
	 * - the words are identified in the constructor, and the iteration
	 *   is performed by {@link #nextWord()}
	 *   
	 * - the subword boundaries are identified in {@link #next()}
	 *   We then set up {@link #found} to contain the most
	 *   recently found subword.
	 *   
	 * - the soft hyphen discarding is processed in {@link #maybeDiscardHyphens()}
	 *   
	 *   - if we are not {@link #alwaysDiscard}ing then {@link #afterDiscard}
	 *   can be set to null to return the non-discarded version on the next cycle.
	 *   
	 */
	private class TermCompletionTokenStream extends TokenStream {

		final String[] words;
		final TermAttribute termAtt;
		
		
		
		char currentWord[] = new char[]{};
		Matcher softMatcher;
		int currentWordIx = -1;
		
		
		int charPos = 0;
		private String afterDiscard;
		private CharBuffer found;
		
		public TermCompletionTokenStream(StringReader reader) {
		    termAtt = addAttribute(TermAttribute.class);
			words = wordBoundary.split(getStringReaderContents(reader));
		}
		
		@Override
		public boolean incrementToken() throws IOException {
			if ( next() ) {
				if (afterDiscard != null) {
					int lg = afterDiscard.length();
					afterDiscard.getChars(0, lg, termAtt.termBuffer(), 0);
				    termAtt.setTermLength(lg);
				} else {
				    int lg = found.length();
					found.get(termAtt.termBuffer(), 0, lg);
				    termAtt.setTermLength(lg);
				}
				return true;
			} else {
				return false;
			}
		}
		
		private boolean next() {
			if (currentWordIx >= words.length) {
				return false;
			}
			if (!alwaysDiscard) {
				// Last match was the discarded version,
				// now do the non-discard version.
				if (afterDiscard != null) {
					afterDiscard = null;
					return true;
				}
			}
			afterDiscard = null;
			if (charPos + 1 < currentWord.length && softMatcher.find(charPos+1)) {
				charPos = softMatcher.end();
				maybeDiscardHyphens();
				return true;
			} else {
				return nextWord();
			}
		}

		void maybeDiscardHyphens() {
			found = CharBuffer.wrap(currentWord, charPos, currentWord.length - charPos);
			Matcher discarding = discard.matcher(found);
			if (discarding.find()) {
				afterDiscard = discarding.replaceAll("");
			}
		}
		
		private boolean nextWord() {
			currentWordIx++;
			if (currentWordIx >= words.length) {
				return false;
			}
			currentWord = words[currentWordIx].toCharArray();
			termAtt.resizeTermBuffer(currentWord.length);
			charPos = 0;
			softMatcher = subWordBoundary.matcher(words[currentWordIx]);
			maybeDiscardHyphens();
			return true;
		}

	}

	static String getStringReaderContents(StringReader reader) {
		try {
			reader.mark(Integer.MAX_VALUE);
			int length = (int) reader.skip(Integer.MAX_VALUE);
			reader.reset();
			char fileContent[] = new char[length];
			reader.read(fileContent);
			reader.reset();
			return new String(fileContent);
		} catch (IOException e) {
			throw new RuntimeException("Impossible",e);
		}
	}
}
