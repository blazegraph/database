package benchmark.generator;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;

public class DateGenerator {
	public static long oneDayInMillis = 24*60*60*1000;
	
	
	private long from, to;
	private Random ranGen;
	
	public DateGenerator(GregorianCalendar from, GregorianCalendar to, Long seed)
	{
		this.from = from.getTimeInMillis();
		this.to = to.getTimeInMillis();
		ranGen = new Random(seed);
	}
	
	/*
	 * Date generator with range from - (from+toSpanInDays) 
	 */
	public DateGenerator(GregorianCalendar from, Integer toSpanInDays, Long seed)
	{
		this.from = from.getTimeInMillis();
		this.to = this.from + oneDayInMillis*toSpanInDays;

		ranGen = new Random(seed);
	}
	
	/*
	 * Date generator with range (to-fromSpanInDays) - to 
	 */
	public DateGenerator(Integer fromSpanInDays, GregorianCalendar to, Long seed)
	{
		this.to = to.getTimeInMillis();
		this.from = this.to - oneDayInMillis*fromSpanInDays;
		ranGen = new Random(seed);
	}
	
	public DateGenerator(Long seed)
	{
		this.from = 0l;
		this.to = 0l;
		ranGen = new Random(seed);
	}
	
	/*
	 * Date between from and to
	 */
	public GregorianCalendar randomDate()
	{
		long date = (long)(ranGen.nextDouble()*(to-from)+from);
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(new Date(date));
		
		return gc;
	}
	
	/*
	 * Date between from and to
	 */
	public Long randomDateInMillis()
	{
		long date = (long)(ranGen.nextDouble()*(to-from)+from);
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(new Date(date));
		
		return gc.getTimeInMillis();
	}
	
	/*
	 * format the date
	 */
	public static String formatDate(GregorianCalendar c)
	{
		int day = c.get(Calendar.DAY_OF_MONTH);
		int month = c.get(Calendar.MONTH)+1;
		int year = c.get(Calendar.YEAR);
		
		String prefixDay = "";
		String prefixMonth = "";		
		
		if(day<10)
			prefixDay = "0";
		
		if(month<10)
			prefixMonth = "0";
		
		return year+"-"+prefixMonth+month+"-"+prefixDay+day;
	}
	
	/*
	 * format the date
	 */
	public static String formatDate(Long date)
	{
		GregorianCalendar c = new GregorianCalendar();
		c.setTimeInMillis(date);
		
		return formatDate(c);
	}
	
	/*
	 * Format date in xsd:dateTime format
	 */
	public static String formatDateTime(Long date) {
		GregorianCalendar c = new GregorianCalendar();
		c.setTimeInMillis(date);
		
		String dateString = formatDate(c);
		return dateString + "T00:00:00";
	}
	
	public static String formatDateTime(GregorianCalendar date) {
		String dateString = formatDate(date);
		return dateString + "T00:00:00";
	}
	
	public Long randomDateInMillis(Long from, Long to)
	{
		long date = (long)(ranGen.nextDouble()*(to-from)+from);
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(new Date(date));
		
		return gc.getTimeInMillis();
	}
}
