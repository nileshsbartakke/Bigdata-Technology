package distinct_word;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class distinct extends Configured implements Tool{
	
		@SuppressWarnings("deprecation")
		public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//Configured conf = new Configured();
		Job job = new Job(getConf(), "Distinct Value");
		job.setJarByClass(getClass());
		job.setMapperClass(word_mapper.class);
		//job.setCombinerClass(DistinctValueReducer.class); //Reducer input and output is diff
		job.setReducerClass(word_reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		return job.waitForCompletion(true) ? 0 : 1;
		}

	public static void main(String[] args) throws Exception {
		
		
		
		int jobStatus = ToolRunner.run(new distinct(),args);        
		System.exit(jobStatus);
		
		
		

	}

}
