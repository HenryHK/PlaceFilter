package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import task3.TextTextPair;

public class TopTagGroupingComparator extends WritableComparator {
    protected TopTagGroupingComparator() {
		super(TextTextPair.class,true);
	}

	/**
	 * Only compare the key when grouping reducer input together
	 */
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextTextPair tip1 = (TextTextPair) w1;
		TextTextPair tip2 = (TextTextPair) w2;
		return tip1.getKey().compareTo(tip2.getKey());
	}
}