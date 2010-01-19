package benchmark.vocabulary;

import java.util.HashMap;

public class RDF{
	//The namespace of this vocabulary as String
	public static final String NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
		
	public static final String PREFIX = "rdf:";
	
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
	
	//Resources
    public static final String Alt =  NS+"Alt";
    public static final String Bag =  NS+"Bag";
    public static final String Property =  NS+"Property";
    public static final String Seq =  NS+"Seq";
    public static final String Statement =  NS+"Statement";
    public static final String List =  NS+"List";
    public static final String nil =  NS+"nil";

    //Properties
    public static final String first =  NS+"first";
    public static final String rest =  NS+"rest";
    public static final String subject =  NS+"subject";
    public static final String predicate =  NS+"predicate";
    public static final String object =  NS+"object";
    public static final String type =  NS+"type";
    public static final String value =  NS+"value";
}