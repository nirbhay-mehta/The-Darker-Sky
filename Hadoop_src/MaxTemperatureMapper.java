package temp;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
	
	String line = value.toString();
	int year=Integer.parseInt(line.substring(15,19));
	String month=line.substring(19, 21);
    int month_value =Integer.parseInt(line.substring(19, 21));
    int date = Integer.parseInt(line.substring(21,23));
    
    if(month_value==7)
    {
        Calendar calendar=Calendar.getInstance();
        calendar.set(year, Calendar.JULY, date);                
        
        int maxDay=calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        int minDay= maxDay -7;
     
        if(date > minDay && date < maxDay)
        {        
            int airTemperature;
            
            if (line.charAt(87) == '+') 
            { 
                    airTemperature = Integer.parseInt(line.substring(88, 92));
            } else {
                    airTemperature = Integer.parseInt(line.substring(87, 92));
            }
            String quality = line.substring(92, 93);
            if (airTemperature != MISSING && quality.matches("[01459]")) {
                    context.write(new Text(month), new IntWritable(airTemperature));
            }
            }
    }
	}
}