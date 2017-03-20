package wikipedia.org;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateTotalLinksReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text word, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum=0;
		// Computing the count of pages extracted from XML data
		for(IntWritable count:values){
			sum=sum+count.get();
		}
		context.write(new Text(word), new IntWritable(sum));
	}
}