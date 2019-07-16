package duplicate_word;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class duplicate extends Configured implements Tool {
	 public int run(String[] arg0) throws Exception {
         // TODO Auto-generated method stub
         @SuppressWarnings("deprecation")
         Job job = new Job(getConf(), "Duplicate value");
         job.setJarByClass(duplicate.class);
         
         job.setMapperClass(duplicate_mapper.class);
         job.setReducerClass(duplicate_reducer.class);
         
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(IntWritable.class);
         
         FileInputFormat.addInputPath(job, new Path(arg0[0]));
         FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
         
         return job.waitForCompletion(true) ? 0 : 1;            
   }
   
	public static void main(String[] args) throws Exception {

        int jobStatus = ToolRunner.run(new duplicate(), args);
        System.out.println(jobStatus);
		
	}

}
