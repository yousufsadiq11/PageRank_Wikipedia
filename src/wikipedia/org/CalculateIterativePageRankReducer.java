package wikipedia.org;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateIterativePageRankReducer extends
		Reducer<Text, Text, Text, Text> {
	String tab_seperator = "\\t";
	String tab = "\t";
	String addText="EliminatingRedLinks";
	String outlinks_passing="!@#";
	public void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {
			String line,complete_line;
			double sum = 0, updatedPageRankValue;
			int flag = 0, empty_string_flag = 0;
			String temp_list[], link_list = "";
			String title = word.toString();
			// Checking if title equals empty string
			if (word.equals(""))
				empty_string_flag = 1;
			// Adding data along with outlinks to the output
			for (Text iterate : values) {
				line = iterate.toString();complete_line=iterate.toString();
				if (line.contains(outlinks_passing)) {
						link_list = line.substring(3,
								line.length());
					continue;
				}
				if (line.contains(addText)) {
					flag = 1;
					continue;
				}
				String splitted[] = complete_line.split(tab_seperator);
				// Computing pageRank summuation over all outlinks
				sum = sum
						+ (Double.parseDouble(splitted[0]) / Double
								.parseDouble(splitted[1]));
			}
			if (flag == 1 && empty_string_flag == 0) {
				// Computing final pageRank
				updatedPageRankValue = 0.85 * sum + (0.15);
				context.write(new Text(title), new Text(updatedPageRankValue
						+ tab + link_list));
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}