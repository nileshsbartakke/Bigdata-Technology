package duplicate_word;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class duplicate_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static final IntWritable one=new IntWritable(1);
	public void map(LongWritable key,Text value,Mapper <LongWritable,Text,Text,IntWritable>.Context context) throws IOException, InterruptedException
	{
		if(key.get() == 0 && value.toString().contains("first_name"))
			{
			return;
			}		
		else
		{
			String values[] =value.toString().split(",");
			context.write(new Text(values[1]), one);
		}
		
		
	}
	
	
	
	
}
