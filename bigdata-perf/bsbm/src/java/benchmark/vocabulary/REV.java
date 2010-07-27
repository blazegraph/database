package benchmark.vocabulary;

import java.util.HashMap;

public class REV {
	
	//The namespace of this vocabulary as String
	public static final String NS = "http://purl.org/stuff/rev#";
	
	public static final String PREFIX = "rev:";
	
	private static HashMap<String, String> uriMap = new HashMap<String, String>();
	
	/*
	 * For prefixed versions
	 */
	public static String prefixed(String string) {
		if(uriMap.containsKey(string)) {
			return uriMap.get(string);
		}
		else {
			String newValue = PREFIX + string;
			uriMap.put(string, newValue);
			return newValue;
		}
	}
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
	
	//Resource: Review
	public static final String Review = NS + "Review";
	
	//Property: reviewer
	public static final String reviewer = NS + "reviewer";
	
	//Property: text
	public static final String text = NS + "text";
}
