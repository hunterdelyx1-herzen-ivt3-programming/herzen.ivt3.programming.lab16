package ru.spb.herzen.ivt3.Third;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class CanadaReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        if (key.toString().startsWith("2010")) {
            int amount = 0;
            float count = 0;
            for (FloatWritable current : values) {
                count += current.get();
                amount++;
            }
            context.write(key, new FloatWritable(count / amount));
        }
    }
}