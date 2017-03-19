package wikipedia.org;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateIterativePageRankReducer extends
		Reducer<Text, Text, Text, Text> {
	String seperator = "\\t";
	String tab = "\t";

	public void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {
			String line,complete_line;
			double sum = 0, updatedPageRankValue;
			int flag = 0, empty_string_flag = 0;
			String temp_list[], link_list = "";
			String title = word.toString();
			if (word.equals(""))
				empty_string_flag = 1;
			for (Text iterate : values) {
				line = iterate.toString();complete_line=iterate.toString();
				if (line.contains("!@#")) {
						link_list = line.substring(3,
								line.length());
					continue;
				}
				if (line.contains("EliminatingRedLinks")) {
					flag = 1;
					continue;
				}
				System.out.println(complete_line);
				String splitted[] = complete_line.split(seperator);
				sum = sum
						+ (Double.parseDouble(splitted[0]) / Double
								.parseDouble(splitted[1]));
			}
			if (flag == 1 && empty_string_flag == 0) {
				updatedPageRankValue = 0.85 * sum + (0.15);
				context.write(new Text(title), new Text(updatedPageRankValue
						+ tab + link_list));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}