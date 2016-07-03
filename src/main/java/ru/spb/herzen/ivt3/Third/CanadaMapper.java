package ru.spb.herzen.ivt3.Third;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class CanadaMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        // 0 = MODEL: YEAR
        // 1 = MAKE
        // 2 = MODEL: # = high output engine
        // 3 = VEHICLE CLASS
        // 4 = ENGINE SIZE
        // 5 = CYLINDERS
        // 6 = TRANSMISSION
        // 7 = FUEL TYPE
        // 8 = FUEL CONSUMPTION: CITY (L/100 km)
        // 9 = FUEL CONSUMPTION: HWY (L/100 km)
        // 10 = FUEL CONSUMPTION: COMB (mpg)
        // 11 = CO2 EMISSIONS: (g/km)
        String[] parsedLine = line.toString().split(",");
        Float amount = Float.parseFloat(parsedLine[8]);
        context.write(new Text(parsedLine[0]), new FloatWritable(amount));
    }
}