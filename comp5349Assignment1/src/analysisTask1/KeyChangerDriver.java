package analysisTask1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//This is the driver to configure KeyChangerMapper to exchange location between place_name and numberOfPhotos
//@see KeyChangerMapper
//@author Zhao Liu

public class KeyChangerDriver 
{
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: Location Changer <in> <out>");
			System.exit(2);
		}
		
//		if (otherArgs.length == 3){
//			conf.set("mapper.placeFilter.country", otherArgs[2]);
//		}
		
		Job job = new Job(conf, "Location Changer");		
		job.setJarByClass(KeyChangerDriver.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(KeyChangerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
