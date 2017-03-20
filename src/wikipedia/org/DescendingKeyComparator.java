package wikipedia.org;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingKeyComparator extends WritableComparator {
	protected DescendingKeyComparator() {
		super(DoubleWritable.class, true);
	}

	@Override
	// Method to retrieve values in descending order
	public int compare(WritableComparable w1, WritableComparable w2) {
		DoubleWritable d1 = (DoubleWritable) w1;
		DoubleWritable d2 = (DoubleWritable) w2;
		int cmp = d1.compareTo(d2);
		return cmp * -1;
	}
}
