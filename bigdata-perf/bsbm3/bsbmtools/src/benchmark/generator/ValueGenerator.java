package benchmark.generator;

import java.security.*;
import java.util.Random;

public class ValueGenerator {
	private Random ranGen;
	
	public ValueGenerator(long seed)
	{
		ranGen = new Random(seed);
	}
	
	/*
	 * Returns an int value between from and to (inclusive)
	 */
	public int randomInt(int from, int to)
	{
		return ranGen.nextInt(to-from+1) + from;
	}
	
	/*
	 * Returns an double value between from and to
	 */
	public double randomDouble(double from, double to)
	{
		return ranGen.nextDouble()*(to-from)+from;
	}
	
	/*
	 * Returns a long value between from and to
	 */
	public long randomLong(long from, long to)
	{
		return (long)ranGen.nextDouble()*(to-from+1) + from;
	}
	
	/*
	 * Returns a random String (a-z) of given length.
	 */
	public String randomTextString(int length)
	{
		char[] temp = new char[length];
		for(int i=0;i<length;i++)
		{
			temp[i] = (char)(ranGen.nextDouble() * ('z'-'a'+1) + 'a');
		}
		
		return new String(temp);
			
	}
	
	public String randomSHA1()
	{
		byte[] b = new byte[4];
		int i = ranGen.nextInt();

		for(int j=0;j<4;j++)
		{
			b[j] = (byte)(i % 256);
			i >>= 8;
		}
		
		MessageDigest md=null;
		try{
			md = MessageDigest.getInstance("SHA1");
		} catch(java.security.NoSuchAlgorithmException e) { System.err.println(e.getMessage()); System.exit(-1);}
		
		byte[] chksum = md.digest(b);
		StringBuffer result=new StringBuffer();
		
		for(int j=0;j<chksum.length;j++)
			result.append(Integer.toHexString(0xFF & chksum[j]));
		
		return result.toString();
	}
	
	
	public static void main(String argv[])
	{
		TextGenerator textGen = new TextGenerator("familynames.txt", 5235332l);
		
		System.out.println(textGen.getRandomSentence(20));
	}
}
