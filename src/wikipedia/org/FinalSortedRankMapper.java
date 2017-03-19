package wikipedia.org;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FinalSortedRankMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {
	String tab_seperator="\\t";
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
		String line = lineText.toString();
		String splitted[]=line.split(tab_seperator);
		context.write(new Text(splitted[0]),new DoubleWritable(Double.parseDouble(splitted[1])));
	}
}