package task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * placeName total
 *
 */
public class TextTextPair implements WritableComparable<TextTextPair>{

	private Text key;
	private Text order;
	
	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getOrder() {
		return order;
	}

	public void setOrder(Text order) {
		this.order = order;
	}	
	
	public TextTextPair(){
		this.key = new Text();
		this.order = new Text();
	}
	
	public TextTextPair(String key, String order){
		this.key = new Text(key);
		this.order = new Text(order);
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		key.readFields(in);
		order.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		key.write(out);
		order.write(out);
	}

	@Override
	public int compareTo(TextTextPair other) {
		// TODO Auto-generated method stub
		int cmp = key.compareTo(other.getKey());
		if (cmp != 0) {
			return cmp;
		}
		//Integer current = Integer.parseInt(order.toString());
		//Integer otherInt = Integer.parseInt(other.order.toString());
		return order.compareTo(other.order);
	}

	@Override
	public int hashCode() {
		return key.hashCode() * 163 + 1;
	}

	public boolean equals(Object other) {
		if (other instanceof TextTextPair) {
			TextTextPair tip = (TextTextPair) other;
			return key.equals(tip.key) && order.equals(tip.order);
		}
		return false;
	}
}
