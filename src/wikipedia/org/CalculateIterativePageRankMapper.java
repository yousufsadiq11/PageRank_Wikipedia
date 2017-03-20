package wikipedia.org;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateIterativePageRankMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	String tab_seperator="\t";
	String addText="EliminatingRedLinks";
	String outlinks_passing="!@#";
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		try {
			context.getInputSplit();
			int length = 0;
			String line = lineText.toString();
			String splitted[] = new String[3];
			splitted[1] = "";
			splitted[0] = "";
			splitted[2] = "";
			splitted = line.split(tab_seperator);
			String page_title = splitted[0];
			String page_rank_value = splitted[1];
			String link_list = "";
			// Checking if the corresponding has any outlinks
			// Skipping the steps if there are no outlinks
			if (splitted.length == 2) {
				context.write(new Text(page_title), new Text(
						addText));
				context.write(new Text(page_title), new Text(page_rank_value
						+ tab_seperator + 1));
				return;
			}
			// Checking if there is empty string in outlinks
			if (!(splitted[2].equals(""))) {
				link_list = splitted[2];
			}
			// Seperating it's corresponding outlinks
			context.write(new Text(page_title), new Text(addText));
			String further_split_all_links[] = link_list.split(";;;");
			length = further_split_all_links.length;
			// Adding outlink followed by pagerank and count of outlinks as output
			// length > 0 indicates there is atleast one outlinks
			if (length > 0) {
				for (int i = 0; i < further_split_all_links.length; i++) {
					context.write(new Text(further_split_all_links[i]),
							new Text(page_rank_value + tab_seperator + length));
				}
			} 
			// Writing 1 as length if there are no outlinks
			else
				context.write(new Text(""),
						new Text(page_rank_value + tab_seperator + 1));
			context.write(new Text(page_title), new Text(outlinks_passing + link_list));
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}