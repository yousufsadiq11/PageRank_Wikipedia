package wikipedia.org;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Driver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		Job job2 = Job.getInstance(getConf(), "Calculate Total Number Of Links");
		job2.setJarByClass(this.getClass());
		// Instantiating it's Mapper, and Reducer classes
		job2.setMapperClass(CalculateTotalLinksMapper.class);
		job2.setReducerClass(CalculateTotalLinksReducer.class);
		// Instantiating input and output paths for Job 2
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		// Instantiating corresponding output classes
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		boolean success = job2.waitForCompletion(true);
		return success ? 0 : 1;
		
		
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Driver(), args);
		System.exit(res);
	}
}
