package com.zbiti.hadoop.study;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * select SELLUNIT_CODE,count(1) c from invoice_external group by SELLUNIT_CODE
 * order by c
 * 
 * @author liujia
 *
 */
public class StatEfpOrder extends Configured implements Tool {
	// group by mapper
	private static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(" ");
			// 总共6列values[1]：销货单位 一行记录一张发票
			if (values.length == 6) {
				context.write(new Text(values[1]), new IntWritable(1));
			}
		}
	}

	// group by reducer
	private static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			// values 指同一个key(销货单位) 对应的values
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	// order by reducer
	private static class OrderReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}

		}

	}

	// order by mapper
	private static class InverseOrderMapper extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			context.write(new IntWritable(Integer.valueOf(values[1])), new Text(values[0]));
		}
	}

	public static class DecreasingIntWritableComparator extends IntWritable.Comparator {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
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
		int res = ToolRunner.run(new Configuration(), new StatEfpOrder(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		Path tempDir = new Path("statefporder-tmp" + Integer.toString(new Random().nextInt(1000))); // 定义一个临时目录
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: StatEfp <in> <out>");
			return 0;
		}
		try {
			delOutput(conf, otherArgs[otherArgs.length - 1]);

			Job jobGroup = Job.getInstance(conf, "efp invoice stats group by SELLUNIT_CODE");
			jobGroup.setJarByClass(StatEfpOrder.class);

			jobGroup.setMapperClass(StatMapper.class);
			jobGroup.setCombinerClass(StatReducer.class);
			jobGroup.setReducerClass(StatReducer.class);
			jobGroup.setOutputKeyClass(Text.class);
			jobGroup.setOutputValueClass(IntWritable.class);
			jobGroup.setNumReduceTasks(1);
			FileInputFormat.addInputPath(jobGroup, new Path(otherArgs[otherArgs.length - 2]));
			FileOutputFormat.setOutputPath(jobGroup, tempDir);
			if (jobGroup.waitForCompletion(true)) {
				Job jobOrder = Job.getInstance(conf, "efp invoice stats order after group by");
				jobOrder.setJarByClass(StatEfpOrder.class);
				jobOrder.setMapperClass(InverseOrderMapper.class);
				jobOrder.setReducerClass(OrderReducer.class);
				jobOrder.setNumReduceTasks(1);

				jobOrder.setMapOutputKeyClass(IntWritable.class);
				jobOrder.setMapOutputValueClass(Text.class);

				jobOrder.setOutputKeyClass(Text.class);
				jobOrder.setOutputValueClass(IntWritable.class);

				FileInputFormat.addInputPath(jobOrder, tempDir);
				FileOutputFormat.setOutputPath(jobOrder, new Path(otherArgs[otherArgs.length - 1]));
				jobOrder.setSortComparatorClass(DecreasingIntWritableComparator.class);
				return (jobOrder.waitForCompletion(true) ? 0 : 1);
			}
			return 1;
		} catch (Exception e) {
			System.out.println(e.getMessage() + "----------------------------------");
			return 1;
		} finally {
			FileSystem.get(conf).deleteOnExit(tempDir);
		}

	}
}
