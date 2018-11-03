package com.twitter.followcountcluster;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class TwitterGraphCluster extends Configured implements org.apache.hadoop.util.Tool {
    private static final Logger logger = LogManager.getLogger(TwitterGraphCluster.class);

    public static void main(String[] args) {
        if(args.length!=2) {
            throw new Error("two arguments are required <input> <output>");
        }
        try {
            ToolRunner.run(new TwitterGraphCluster(), args);
        } catch(final Exception e) {
            logger.error("", e);
        }
    }
    @Override
    public int run(String[] args) throws Exception {

        //if output file already exists delete the output file
        File f = new File(args[1]);
        if(f.exists() && f.isDirectory()) {
            FileUtils.forceDelete(f);
        }

        //initial job configuration
        final Configuration conf = new Configuration();
        final Job job =Job.getInstance(conf, "FollowCount Job");
        job.setJarByClass(TwitterGraphCluster.class);
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(FollowCountMapper.class);
        job.setReducerClass(FollowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set class types for input and output values
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return 0;
    }

    public static class FollowCountMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            context.write(new Text(data[1]), new Text("1"));
        }

    }

    public static class FollowCountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final Text val : values) {
                sum += 1;
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }
}
