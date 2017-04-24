package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import task3.TagMapper;
import task3.TagReducer;

public class TagDriver 
{
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: TagDriver <in> <out>");
			System.exit(2);
		}
		
		
		Job job = new Job(conf, "Tag Driver");
		job.setJarByClass(TagDriver.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(TagMapper.class);
		job.setReducerClass(TagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(KeyInputComparator.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
