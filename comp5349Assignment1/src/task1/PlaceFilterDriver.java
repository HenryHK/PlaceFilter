package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//This is the driver to configure PlaceFilterMapper to select necessary place data out based on input file (place.txt)
//@see PlaceFilterMapper
//@author Zhao Liu

public class PlaceFilterDriver 
{
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: PlaceFilter <in> <out>");
			System.exit(2);
		}
		
		
		Job job = new Job(conf, "Place Filter");
		job.setJarByClass(PlaceFilterDriver.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(PlaceFilterMapper.class);
		job.setReducerClass(PlaceFilterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}