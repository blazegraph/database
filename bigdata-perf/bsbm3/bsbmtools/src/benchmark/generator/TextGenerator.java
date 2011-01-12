package benchmark.generator;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class TextGenerator {
	RandomAccessFile dictionary;
	private Random ranGen;
	private HashMap<String,Integer> logList;//The word list for the Test Driver
	private Vector<String> words;//For faster access, save all words in Vector-Array
	
	private final static int FINDSTART = 1;
	private final static int READWORD = 2;
	private final static int FINISHED = 3;
	private final static int EOF = 4;

	public TextGenerator(String file)
	{
		ranGen = new Random();

		init(file);
	}
	
	public TextGenerator(String file, long seed)
	{
		ranGen = new Random(seed);
		
		init(file);
	}
	
	//Initialize this TextGenerator
	private void init(String file) {
		try {
			dictionary = new RandomAccessFile(file,"r");
			
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		System.out.print("Reading in " + file + ": ");
		logList = null;
			
		createWordList();
		
		try {
			dictionary.close();
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	//Generates a Vector of words
	@SuppressWarnings("fallthrough")
    private void createWordList() {
		words = new Vector<String>();

		while(true) {
			StringBuffer word = new StringBuffer();
			try{
				int state = FINDSTART;
				char c=' ';
				
				while(state!=FINISHED)
				{
					try {
						c=(char)dictionary.readByte();
					} catch(EOFException eof) {
						state = EOF;
						break;
					}
	
					switch(state)
					{
						case FINDSTART:
							if(isLetter(c))
								state = READWORD;//is a letter, go on with READWORD
						case READWORD:
							if(isLetter(c))
								word.append(c);
							else
								state = FINISHED;
					}
				}
				
				//Check if a word has been read in
				if(word.length()>0) {
					String wordString = word.toString();
					words.add(wordString);
				}
					
				//Finish at EOF
				if(state==EOF)
					break;
				
			} catch(IOException e) {
				System.err.println("Couldn't get word.\n"+e.getMessage());
			}
		}
		System.out.println(words.size() + " words read in.");
	}
	
	//reads a random word from the text file
	private String getRandomWord()
	{
		int index = ranGen.nextInt(words.size());
		String word = words.elementAt(index);

		if(logList!=null) 
			addWordToWordlist(word);
		
		return word;
	}
	
	private void addWordToWordlist(String word) {
		Integer count = 1;
		if(logList.containsKey(word)) {
			count = logList.get(word);
			count++;
		}
		logList.put(word, count);
	}
	
	/*
	 * returns a random sentence with number words from the chosen dictionary.
	 */
	public String getRandomSentence(int numberWords)
	{
		StringBuffer sentence = new StringBuffer();
		
		if(numberWords>0)
			sentence.append(getRandomWord());
		
		for(int i=1;i<numberWords;i++) {
			sentence.append(" ");
			sentence.append(getRandomWord());
		}
		
		return sentence.toString();
	}
	
	private boolean isLetter(char c)
	{
		return Character.isLetter(c) || c=='-';
	}
	
	/*
	 * Generate a homepage for a producer
	 */
	public static String getProducerWebpage(int producerNr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append("http://www.Producer");
		s.append(producerNr);
		s.append(".com/");
		
		return s.toString();
	}
	
	/*
	 * Generate a homepage for a vendor
	 */
	public static String getVendorWebpage(int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append("http://www.vendor");
		s.append(vendorNr);
		s.append(".com/");
		
		return s.toString();
	}
	
	public HashMap<String, Integer> getWordList() {
		return logList;
	}

	public void activateLogging(HashMap<String, Integer> logList) {
		this.logList = logList;
	}
	
	public void deactivateLogging() {
		this.logList = null;
	}
}
