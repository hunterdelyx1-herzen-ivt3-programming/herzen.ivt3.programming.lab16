package ru.spb.herzen.ivt3.First;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

class CountAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable ONE = new IntWritable(1);
    private final Text aKey = new Text("a");

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(line.toString(), " \t\n\r\f,.:;?![]'");
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase().trim();
            if (word.startsWith("a")) context.write(aKey, ONE);
        }
    }
}