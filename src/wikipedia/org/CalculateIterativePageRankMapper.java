package wikipedia.org;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateIterativePageRankMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	// private static final Pattern matching_pattern =
	// Pattern.compile("\\[.+?\\]");
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
try{
		context.getInputSplit();
		int length = 0;
		String line = lineText.toString();
		String splitted[] = new String[3] ;
		splitted[1]="";
		splitted[0]="";
		splitted[2]="";
		System.out.println("asxaascac");
		splitted = line.split("\t");System.out.println(splitted.length);
		String page_title = splitted[0];
		String page_rank_value = splitted[1];
		String link_list = "";if (splitted.length==2)
			return;
		if (!(splitted[2].equals(""))) {
			link_list = splitted[2];
			System.out.println(page_title + " " + page_rank_value + " "
					+ link_list);
		}
		context.write(new Text(page_title), new Text("################"));
		
		String further_split_all_links[] = link_list.split(";");
		System.out.println("furtherrrrrrrrrrr        "
				+ further_split_all_links[0]);
		length = further_split_all_links.length;
		if (!(length == 0)) {
			for (String x : further_split_all_links) {
				context.write(new Text(x), new Text(page_rank_value + "\t"
						+ length));
				System.out.println(length + "loop" + x + page_rank_value
						+ length);
			}
		} else
			context.write(new Text(" "), new Text(page_rank_value + "\t" + 1));
		context.write(new Text(page_title), new Text(link_list));
	}catch(Exception e){
		e.printStackTrace();
	}
	}
	
}