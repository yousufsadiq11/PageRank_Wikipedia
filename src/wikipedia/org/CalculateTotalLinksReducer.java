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

public class CalculateTotalLinksReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text word, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable count:values){
			sum=sum+count.get();
		}
		context.write(new Text(word), new IntWritable(sum));
	}
}