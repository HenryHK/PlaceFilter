package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TagPlaceJoinGroupingComparator extends WritableComparator {
    protected TagPlaceJoinGroupingComparator() {
		super(TextIntPair.class,true);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Only compare the key when grouping reducer input together
	 */
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextIntPair tip1 = (TextIntPair) w1;
		TextIntPair tip2 = (TextIntPair) w2;
		return tip1.getKey().compareTo(tip2.getKey());
	}
}