package com.bigdata.search;

import java.util.Locale;


/**
 * This is an implementation of RFC 4647 language range,
 * targetted at the specific needs within bigdata, and only
 * supporting the extended filtering specified in section 3.3.2
 * <p>
 * Language ranges are comparable so that
 * sorting an array and then matching a language tag against each
 * member of the array in sequence will give the longest match.
 * i.e. the longer ranges come first.
 * @author jeremycarroll
 *
 */
public class LanguageRange implements Comparable<LanguageRange> {
	
	private final String range[];
	final String full;
	/**
	 * Note range must be in lower case, this is not verified.
	 * @param range
	 */
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
	
	/**
	 * This implements the algoirthm of section 3.3.2 of RFC 4647
	 * as modified with the observation about private use tags
	 * in <a href="http://lists.w3.org/Archives/Public/www-international/2014AprJun/0084">
	 * this message</a>.
	 * 
	 * 
	 * @param langTag The RFC 5646 Language tag in lower case
	 * @return The result of the algorithm
	 */
	public boolean extendedFilterMatch(String langTag) {
		return extendedFilterMatch(langTag.toLowerCase(Locale.ROOT).split("-"));
	}

	// See RFC 4647, 3.3.2
	boolean extendedFilterMatch(String[] language) {
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