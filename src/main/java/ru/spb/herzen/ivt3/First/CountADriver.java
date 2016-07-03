package ru.spb.herzen.ivt3.First;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

class CountADriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Path output = new Path("./output");

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(output, true);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "\'a\'");

        job.setJarByClass(First.class);

        job.setMapperClass(CountAMapper.class);
        job.setCombinerClass(CountACombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("./input/catcher"));
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
        return 0;
    }
}