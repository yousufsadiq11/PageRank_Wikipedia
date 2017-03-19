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

public class Driver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job1 = Job
				.getInstance(getConf(), "Calculate Total Number Of Links");
		job1.setJarByClass(this.getClass());
		// Instantiating it's Mapper, and Reducer classes
		job1.setMapperClass(CalculateTotalLinksMapper.class);
		job1.setReducerClass(CalculateTotalLinksReducer.class);
		// Instantiating input and output paths for Job 2
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "driver"));
		// Instantiating corresponding output classes
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		boolean success = job1.waitForCompletion(true);
		success = InitialPageRank(args);
		success=Iteration(10, args);
		success=sortingPageRank(args);
		return success ? 0 : 1;

	}

	private boolean sortingPageRank(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String input=args[1] + "Iteration_count_10";
		String output= args[1]+ "Sorted_PageRank";
		Job job4 = Job.getInstance(getConf(), "Sorting Page Rank Values");
		job4.setJarByClass(this.getClass());
		// Instantiating Mapper and Reducer Classes
		//job3.setSortComparatorClass(DescendingKeyComparator.class);
		job4.setMapperClass(FinalSortedRankMapper.class);
		//job3.setReducerClass(FinalSortedRankReducer.class);
		// Instantiating File input and output paths
		FileInputFormat.addInputPath(job4, new Path(input));
		FileOutputFormat.setOutputPath(job4, new Path(output));
		// Instantiating corresponding Output classes
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(DoubleWritable.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		boolean success = job4.waitForCompletion(true);
		return success;
		
	}

	private boolean Iteration(int i, String args[]) throws Exception {
		// TODO Auto-generated method stub
		boolean check = false;
		for (int iterations = 0; iterations < 10; iterations++) {
			String input=args[1] + "Iteration_count_"+ Integer.toString(iterations);
			String output= args[1]+ "Iteration_count_" + Integer.toString(iterations + 1);
			check = computeIterativePageRank(input,output);
		}
		return check;
}

	private boolean computeIterativePageRank(String input,String output)
			throws Exception {
		// TODO Auto-generated method stub
		boolean success=false;
			System.out.println(input+" "+output);
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
			
		//}
		return success;
	}

	public boolean InitialPageRank(String[] args)
			throws Exception {
		Configuration conf = new Configuration();
		double title_count = 0;

		BufferedReader in = new BufferedReader(new FileReader(args[1]
				+ "driver/part-r-00000"));
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

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Driver(), args);
		System.exit(res);
	}
}
/*String splitted[] = {""} ;
		splitted[1]="";
		splitted[0]="";
		splitted[2]="";
		
		
context.getInputSplit();
int length = 0;
String line = lineText.toString();
String splitted[] = { "" };
System.out.println("asxaascac");
splitted = line.split("\t");
String page_title = splitted[0];
String page_rank_value = splitted[1];
String link_list="";
if(!(splitted[2].equals(""))){
link_list = splitted[2];System.out.println(page_title+" "+page_rank_value+" "+link_list);}
context.write(new Text(page_title), new Text("################"));
if (link_list.length()==0)
	return;
String further_split_all_links[] = link_list.split(";");
System.out.println("furtherrrrrrrrrrr        "+further_split_all_links[0]);
length = further_split_all_links.length;
if (!(length == 0)) {
	for (String x:further_split_all_links){
		context.write(new Text(x), new Text(
				page_rank_value + "\t" + length));
		System.out.println(length+"loop"+x+page_rank_value+length);	
	}
} else
	context.write(new Text(""), new Text(page_rank_value + "\t" + 1));
context.write(new Text(page_title), new Text(link_list));
*/