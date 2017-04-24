package task3;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class TopTagPartitioner extends Partitioner<TextTextPair,Text>{

    @Override
	public int getPartition(TextTextPair key, Text value, int numPartition) {
		// TODO Auto-generated method stub
		return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartition;
	}
	

}