package count_record;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


private static final IntWritable  one = new IntWritable(1);
private Text record = new Text("Record");
@Override
protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)
        throws IOException, InterruptedException 
{
       
        if (key.get() == 0 && value.toString().contains("first_name")) {
                return;
        } else 
                context.write(record, one);
}
}