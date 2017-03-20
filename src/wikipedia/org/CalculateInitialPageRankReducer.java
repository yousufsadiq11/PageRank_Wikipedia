package wikipedia.org;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateInitialPageRankReducer extends
		Reducer<Text, Text, Text, Text> {
	String seperator = "\t";
	String list_seperator = ";;;";

	public void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Setting Initial Page Rank as 1/N
		// 'N' is the number of pages in the corpus
		Configuration conf=context.getConfiguration();double tempLength=conf.getDouble("length",0);
		Double Initial_pageRank = 1/tempLength;
		String sum = "";
		sum = sum + String.valueOf(Initial_pageRank) + seperator;
		int i = 0;
		// Seperating outlinks of a corresponding title with ;;; delimeter
		if (!values.toString().isEmpty()) {
			for (Text count : values) {
				if (i == 0) {
					sum = sum + count.toString();
					i++;
				} else
					sum = sum + list_seperator + count.toString();
			}
			context.write(new Text(word), new Text(sum));
		} else
			context.write(new Text(word), new Text(sum + list_seperator));
	}
}