package com.mapr;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ViewsSumByProduct {

    public static class ViewsSumMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable productID = new IntWritable();
        private final static Text sum = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            System.out.println(line);
            String[] st1 = line.split(",");

            if (st1[2].equalsIgnoreCase("browse") || st1[2].equalsIgnoreCase("click")) {
                productID.set(Integer.parseInt(st1[1]));
                if(st1[2].equalsIgnoreCase("browse")){
                    sum.set("1,0,1");
                }
                else{
                    sum.set("0,1,1");
                }
                context.write(productID,sum);
            }
            else
                return;
        }
    }

    public static class ViewsSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        private Text result = new Text();


        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int views = 0;
            int bcount = 0;
            int ccount = 0;
            for (Text val : values) {
                String[] strVal = val.toString().split(",");
                bcount+=Integer.parseInt(strVal[0]);
                ccount+=Integer.parseInt(strVal[1]);
                views+=Integer.parseInt(strVal[2]);
            }
            result.set(bcount+","+ccount+","+views);
            System.out.println(key +"=>" + result);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "ViewsSumByProduct");
        job.setJarByClass(ViewsSumByProduct.class);

        // Mapper configuration
        job.setMapperClass(ViewsSumMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(ViewsSumReducer.class);

        job.setReducerClass(ViewsSumReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
