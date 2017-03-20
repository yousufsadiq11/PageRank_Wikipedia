package wikipedia.org;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalSortedRankReducer extends
		Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	public void reduce(DoubleWritable word, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// Writing Values from the Mapper to output
		// Compare method is overridden in DescendingKeyComparator Class which sorts pageRanks in descending order
		try {
			for (Text val : values) {
				context.write(val, word);
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}