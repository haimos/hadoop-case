package com.zbiti.hadoop.study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 
 * @author liujia
 *
 */
public class StatEfp extends Configured implements Tool {
	private static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(" ");
			if (values.length == 6) {
				context.write(new Text(values[1]), new IntWritable(1));
			}
		}
	}

	private static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}


	private static void delOutput(Configuration conf, String dirPath) throws IOException {

		Path targetPath = new Path(dirPath);
		FileSystem fs = targetPath.getFileSystem(conf);
		if (fs.exists(targetPath)) {
			fs.delete(targetPath, true);

		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StatEfp(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: StatEfp <in> <out>");
			return 0;
		}
        //处理输出文件
		delOutput(conf, otherArgs[otherArgs.length - 1]);

		Job job = Job.getInstance(conf, "efp-dev-database invoice stats");
		job.setJarByClass(StatEfp.class);

		job.setMapperClass(StatMapper.class);
		job.setCombinerClass(StatReducer.class);
		job.setReducerClass(StatReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		//输入文件
		FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
		//输出文件
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		boolean result = job.waitForCompletion(true);
		return (result ? 0 : 1);

	}
}
