package mapReduce.twiiter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class FollowCount extends Configured implements Tool 

{ 
	private static final Logger logger = LogManager.getLogger(FollowCount.class);
	public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final Text follower = new Text();
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			/*final StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}*/
			
			String[] followData = value.toString().split(",");
			follower.set(followData[1]);
			context.write(follower, one);
		}
		
		
	}

	public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	public int run(final String[] args) throws Exception 
    {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Twitter Count");
        job.setJarByClass(FollowCount.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        job.setMapperClass(MapClass.class);
		job.setCombinerClass(ReducerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
        	
    }
	

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			//ToolRunner.run(tool, args)
			ToolRunner.run(new FollowCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}
