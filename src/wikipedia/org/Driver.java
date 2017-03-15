package wikipedia.org;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
		
		Job job1 = Job.getInstance(getConf(), "Calculate Total Number Of Links");
		job1.setJarByClass(this.getClass());
		// Instantiating it's Mapper, and Reducer classes
		job1.setMapperClass(CalculateTotalLinksMapper.class);
		job1.setReducerClass(CalculateTotalLinksReducer.class);
		// Instantiating input and output paths for Job 2
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"driver"));
		// Instantiating corresponding output classes
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		boolean success = job1.waitForCompletion(true);
		success=InitialPageRank(args);
		return success ? 0 : 1;
		
		
	}
	public boolean InitialPageRank(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		   Configuration conf=new Configuration();double title_count=0;
	
			    BufferedReader in = new BufferedReader(new FileReader(args[1]+"driver/part-r-00000"));
			    String str;
			    while ((str = in.readLine()) != null)
			        {String splitted[]=str.split("\t");
			        title_count=Double.parseDouble(splitted[1]);break;
			        }
			    in.close();
		     conf.setDouble("length", title_count);
		Job job2 = Job.getInstance(conf, "Calculate Initial Page Rank");
		job2.setJarByClass(this.getClass());
		// Instantiating it's Mapper, and Reducer classes
		job2.setMapperClass(CalculateInitialPageRankMapper.class);
		job2.setReducerClass(CalculateInitialPageRankReducer.class);
		// Instantiating input and output paths for Job 2
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"Iteration_count_0"));
		// Instantiating corresponding output classes
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		boolean success = job2.waitForCompletion(true);
		return success;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Driver(), args);
		System.exit(res);
	}
}
