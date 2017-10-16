package com.zbiti.hadoop.study;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BookCateTopN extends Configured implements Tool {
	private static class TreeMapKey implements Comparable<TreeMapKey> {
		private int count;
		private String cateId;

		public int getCount() {
			return count;
		}

		public String getCateId() {
			return cateId;
		}

		public TreeMapKey(int count, String cateId) {
			this.count = count;
			this.cateId = cateId;
		}

		public int compareTo(TreeMapKey o) {
			int firstCompare = -(this.count - o.count);
			if (firstCompare == 0) {
				return -this.cateId.compareTo(o.cateId);
			}
			return firstCompare;
		}

	}

	private static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			// 取内容第10列(即分类id)为key

			if (values.length > 10) {
				context.write(new Text(values[10]), new IntWritable(1));
			}
		}
	}

	private static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private TreeMap<TreeMapKey, Integer> treeMap = new TreeMap<TreeMapKey, Integer>();
		private int k = 10;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			treeMap.put(new TreeMapKey(sum, key.toString()), sum);
			if (treeMap.size() > k) {
				treeMap.remove(treeMap.lastKey());
			}
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Iterator titer = treeMap.entrySet().iterator();
			while (titer.hasNext()) {
				Map.Entry en = (Map.Entry) titer.next();
				TreeMapKey mapKey = (TreeMapKey) en.getKey();
				String cateId = mapKey.getCateId();
				int count = mapKey.getCount();
				context.write(new Text(cateId), new IntWritable(count));
			}
		}

	}

	private static class JoinMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 获取输入文件的全路径和名称
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();
			String[] values = value.toString().split("\t");
			if (path.contains("part-r")) {
				context.write(new Text("topca#" + values[0]), new Text(values[1]));
			} else {
				context.write(new Text("cate#" + values[0]), new Text(values[1]));
			}
		}

	}

	// order by reducer
	private static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		Map<TreeMapKey, Integer> treeMap = new TreeMap<TreeMapKey, Integer>();
		Map<String, String> cate = new HashMap<String, String>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] keys = key.toString().split("#");
			if (key.toString().contains("topca#")) {
				for (Text count : values) {
					TreeMapKey mapKey=new TreeMapKey(Integer.valueOf(count.toString()), keys[1]);
					treeMap.put(mapKey, Integer.valueOf(count.toString()));
				}
			} else {
				for (Text name : values) {
					cate.put(keys[1], name.toString());
				}
			}

		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator titer = treeMap.entrySet().iterator();
			while (titer.hasNext()) {
				Map.Entry en = (Map.Entry) titer.next();
				TreeMapKey key = (TreeMapKey) en.getKey();
				Integer val = (Integer) en.getValue();
				String name = cate.get(key.getCateId());
				context.write(new Text(key.getCateId()), new Text(val + "\t" + name));
			}

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
		int res = ToolRunner.run(new Configuration(), new BookCateTopN(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: book cate <in> <out>");
			return 0;
		}
		try {
			delOutput(conf, otherArgs[otherArgs.length - 1]);
			delOutput(conf, otherArgs[otherArgs.length - 2]);//
			Job jobGroup = Job.getInstance(conf, "book cate count top 10");
			jobGroup.setJarByClass(BookCateTopN.class);

			jobGroup.setMapperClass(StatMapper.class);
			jobGroup.setReducerClass(StatReducer.class);
			jobGroup.setCombinerClass(StatReducer.class);

			jobGroup.setOutputKeyClass(Text.class);
			jobGroup.setOutputValueClass(IntWritable.class);
			jobGroup.setNumReduceTasks(1);

			FileInputFormat.addInputPath(jobGroup, new Path(otherArgs[otherArgs.length - 3]));

			FileOutputFormat.setOutputPath(jobGroup, new Path(otherArgs[otherArgs.length - 2]));// 这个job的中间结果存储文件

			if (jobGroup.waitForCompletion(true)) {
				Job jobOrder = Job.getInstance(conf, "book cate order left join");

				jobOrder.setJarByClass(BookCateTopN.class);
				jobOrder.setMapperClass(JoinMapper.class);
				jobOrder.setReducerClass(JoinReducer.class);
				jobOrder.setNumReduceTasks(1);
				jobOrder.setOutputKeyClass(Text.class);
				jobOrder.setOutputValueClass(Text.class);

				FileInputFormat.addInputPath(jobOrder, new Path(otherArgs[otherArgs.length - 2]));//中间文件
				FileInputFormat.addInputPath(jobOrder, new Path("/user/liujia/case/data/book/cate"));//分类信息
				
				FileOutputFormat.setOutputPath(jobOrder, new Path(otherArgs[otherArgs.length - 1]));
				return (jobOrder.waitForCompletion(true) ? 0 : 1);
			}
			return 1;
		} catch (Exception e) {
			System.out.println(e.getMessage() + "----------------------------------");
			return 1;
		} finally {
		}

	}
}
