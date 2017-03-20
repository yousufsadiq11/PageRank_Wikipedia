package wikipedia.org;

import java.io.BufferedReader;
import java.io.FileReader;
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

public class Driver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// Calling Jobs
		boolean success = calculateNumberOfTitles(args);
		success = calculateInitialPageRank(args);
		success = computeIterations(10, args);
		success = sortPageRank(args);
		return success ? 0 : 1;
	}
	
	// Job to calculate total Number of Pages
	private boolean calculateNumberOfTitles(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1 = Job
				.getInstance(getConf(), "Calculate Total Number Of Links");
		job1.setJarByClass(this.getClass());
		// Instantiating it's Mapper, and Reducer classes
		job1.setMapperClass(CalculateTotalLinksMapper.class);
		job1.setReducerClass(CalculateTotalLinksReducer.class);
		// Instantiating input and output paths for Job 1
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "File_Count"));
		// Instantiating corresponding output classes
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		return job1.waitForCompletion(true);
	}
	
	// Job to sort pageRank in descending order
	private boolean sortPageRank(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// Setting it's input and output paths
		String input = args[1] + "Iteration_count_10";
		String output = args[1] + "Sorted_PageRank";
		Job job4 = Job.getInstance(getConf(), "Sorting Page Rank Values");
		job4.setJarByClass(this.getClass());
		// Instantiating Mapper and Reducer Classes
		job4.setSortComparatorClass(DescendingKeyComparator.class);
		job4.setMapperClass(FinalSortedRankMapper.class);
		job4.setReducerClass(FinalSortedRankReducer.class);
		// Instantiating File input and output paths
		FileInputFormat.addInputPath(job4, new Path(input));
		FileOutputFormat.setOutputPath(job4, new Path(output));
		// Instantiating corresponding Output classes
		job4.setMapOutputKeyClass(DoubleWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		boolean success = job4.waitForCompletion(true);
		return success;
	}

	// Method to compute 'n' Iterations
	private boolean computeIterations(int n, String args[]) throws Exception {
		// TODO Auto-generated method stub
		boolean check = false;
		for (int iterations = 0; iterations < n; iterations++) {
			String input = args[1] + "Iteration_count_"
					+ Integer.toString(iterations);
			String output = args[1] + "Iteration_count_"
					+ Integer.toString(iterations + 1);
			check = computeIterativePageRank(input, output);
		}
		return check;
	}

	// Job to compute Iterative Page Ranks
	private boolean computeIterativePageRank(String input, String output)
			throws Exception {
		// TODO Auto-generated method stub
		boolean success = false;
		Job job3 = Job.getInstance(getConf(), "Calculate Iterative Page Rank");
		job3.setJarByClass(Driver.class);
		// Instantiating it's Mapper, and Reducer classes
		job3.setMapperClass(CalculateIterativePageRankMapper.class);
		job3.setReducerClass(CalculateIterativePageRankReducer.class);
		// Instantiating input and output paths for Job 2
		FileInputFormat.addInputPath(job3, new Path(input));
		FileOutputFormat.setOutputPath(job3, new Path(output));
		// Instantiating corresponding output classes
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		success = job3.waitForCompletion(true);
		return success;
	}

	// Job to calculate Initial Page Rank
	public boolean calculateInitialPageRank(String[] args) throws Exception {
		// Configuration object containing Size of the corpus from Job 1
		Configuration conf = new Configuration();
		double title_count = 0;
		BufferedReader in = new BufferedReader(new FileReader(args[1]
				+ "File_Count/part-r-00000"));
		String str;
		while ((str = in.readLine()) != null) {
			String splitted[] = str.split("\t");
			title_count = Double.parseDouble(splitted[1]);
			break;
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
		FileOutputFormat.setOutputPath(job2, new Path(args[1]
				+ "Iteration_count_0"));
		// Instantiating corresponding output classes
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		boolean success = job2.waitForCompletion(true);
		return success;
	}

	// Invokes run method to run Jobs
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Driver(), args);
		System.exit(res);
	}
}