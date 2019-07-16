package duplicate_word;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

public class duplicate_reducer extends Reducer<Text, IntWritable, Text, NullWritable>{

	public void reduce(Text key,Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException
	{
		
		if(Iterables.size(values) >= 1)
		{
			context.write(key, NullWritable.get());
		}
		
		
		
	}
	
	
	
	
	
}
