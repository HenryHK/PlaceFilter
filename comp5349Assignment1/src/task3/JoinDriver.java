package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import task3.CustomGroupingComparator;
import task3.CustomPartitioner;

public class JoinDriver{
    public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: JoinDriver <in1> <in2> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "join place photo");
		job.setNumReduceTasks(1); 
		job.setJarByClass(JoinDriver.class);
		
		Path photoTable = new Path(otherArgs[1]);
		Path placeTable = new Path(otherArgs[0]);
		Path outputTable = new Path(otherArgs[2]);
		MultipleInputs.addInputPath(job, photoTable, 
				TextInputFormat.class,PhotoMapper.class);
		MultipleInputs.addInputPath(job, placeTable, TextInputFormat.class,PlaceMapper.class);
		FileOutputFormat.setOutputPath(job, outputTable);
		job.setMapOutputKeyClass(TextIntPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(CustomGroupingComparator.class);
		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}