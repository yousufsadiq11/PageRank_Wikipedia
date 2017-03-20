package wikipedia.org;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalSortedRankMapper extends
		Mapper<LongWritable, Text, DoubleWritable, Text> {
	String tab_seperator = "\\t";

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
		try {
			String line = lineText.toString();
			String splitted[] = line.split(tab_seperator);
			context.write(new DoubleWritable(Double.parseDouble(splitted[1])),
					new Text(splitted[0]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}