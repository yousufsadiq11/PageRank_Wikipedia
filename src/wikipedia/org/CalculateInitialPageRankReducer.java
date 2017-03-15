package wikipedia.org;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateInitialPageRankReducer extends
		Reducer<Text, Text, Text, Text> {
	String seperator = "\t";
	String list_seperator = ";";

	public void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Double Initial_pageRank = 1.0d;
		System.out.println("rreddsdsavlksnvdnvksnv");
		String sum = "";
		sum = sum + String.valueOf(Initial_pageRank) + seperator;
		int i = 0;
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