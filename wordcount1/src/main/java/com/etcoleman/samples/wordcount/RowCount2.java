package com.etcoleman.samples.wordcount;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by etcoleman on 4/18/15.
 */
public class RowCount2 {

  private static enum CountersEnum { NUM_ROW_IDS }

  public static class TokenizeMapper extends Mapper<Object,Text,Text,IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text rowId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        rowId.set(itr.nextToken());
        context.write(rowId, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    private MultipleOutputs outputs;

    public void setup(Context context){
      outputs = new MultipleOutputs(context);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      outputs.close();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      result.set(sum);
      // context.write(key, result);

      outputs.write("rows", key, result);

      Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_ROW_IDS.toString());
      counter.increment(sum);

    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "rowId count");

    MultipleOutputs.addNamedOutput(job,"stats", TextOutputFormat.class, Text.class, LongWritable.class);

    MultipleOutputs.addNamedOutput(job,"rows", TextOutputFormat.class, Text.class, LongWritable.class);

    job.setJarByClass(RowCount2.class);
    job.setMapperClass(TokenizeMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean success = job.waitForCompletion(true);

    Counters counters = job.getCounters();

    Counter rows = counters.findCounter(CountersEnum.NUM_ROW_IDS);
    System.out.println("ROWS: " + rows.getValue());

    FileSystem fs = FileSystem.get(conf);
    Path p = new Path(args[1] + "/stats.txt");
    OutputStream os = null;
    PrintWriter writer = null;

    try {
      os = fs.create(p);

      writer = new PrintWriter(os);
      writer.println(rows.getDisplayName() + ": " + rows.getValue());
    } finally {
      writer.close();
    }

    System.exit(success ? 0 : 1);

  }
}
