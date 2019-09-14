package com.mapr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderBySumDesc {

    public static class InverseProductSumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text sum, Context context) throws IOException, InterruptedException {

            context.write(new Text(sum.toString().split("\t")[1]), new IntWritable(Integer.parseInt(sum.toString().split("\t")[0])));
        }
    }

    public static class LimitReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

        private int recordCount = 0;
        private IntWritable productID;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for (IntWritable value : values) {
                productID = new IntWritable(value.get());
                System.out.println("Key ::" + key + "Value ::" + value);
                if(recordCount < 10) {
                    context.write(productID,key);
                    recordCount = recordCount+1;
                }
            }
        }
    }

    public static class DescendingViewsComparator extends WritableComparator {

        public DescendingViewsComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = new IntWritable(Integer.parseInt(w1.toString().split(",")[2]));
            IntWritable key2 = new IntWritable(Integer.parseInt(w2.toString().split(",")[2]));;
            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "OrderBySum");
        job.setJarByClass(ViewsSumByProduct.class);

        job.setMapperClass(InverseProductSumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(DescendingViewsComparator.class);

        job.setReducerClass(LimitReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}