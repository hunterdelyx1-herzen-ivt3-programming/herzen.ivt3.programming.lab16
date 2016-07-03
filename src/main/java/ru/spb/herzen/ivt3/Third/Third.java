package ru.spb.herzen.ivt3.Third;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Third {
    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), (Tool) new CanadaDriver(), new String[]{});
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}