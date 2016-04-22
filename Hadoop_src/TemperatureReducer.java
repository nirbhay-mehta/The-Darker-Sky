package temp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException
	{
		
		int maxValue = Integer.MIN_VALUE;
		int max_temp = 0; 
        int count = 0;
        int avg_temp = 0;
        int difference = 0;
        
		for (IntWritable value : values) 
		{
			maxValue = Math.max(maxValue, value.get());
			
			max_temp += value.get();     
        	count+=1;
		}
		avg_temp = max_temp/count;    
		difference = maxValue - avg_temp;
        context.write(key, new IntWritable(difference));
        
	}
	
}
