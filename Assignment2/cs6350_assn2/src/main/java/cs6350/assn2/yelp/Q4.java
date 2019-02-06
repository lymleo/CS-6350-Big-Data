package cs6350.assn2.yelp;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q4 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("please check the output dir");
			System.exit(2);
		}
		Job job = Job.getInstance(conf);
		job.setJobName("Q4");
		job.setJarByClass(Q4.class);
		job.setMapperClass(Q4Mapper.class);
		job.setCombinerClass(Q4Combiner.class);
		job.setReducerClass(Q4Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Q4Mapper extends Mapper<LongWritable, Text, Text, SumCount> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String delims = "^";
			String[] reviewData = StringUtils.split(value.toString(), delims);

			if (reviewData.length == 4) {
				context.write(new Text(reviewData[2]), new SumCount(Double.parseDouble(reviewData[3]), 1));
			}
		}
	}
	
	public static class Q4Combiner extends Reducer<Text, SumCount, Text, SumCount> {

		@Override
		public void reduce(Text key, Iterable<SumCount> values, Context context)
				throws IOException, InterruptedException {
			SumCount totalSumCount = new SumCount();
			for (SumCount sumCount : values)
				totalSumCount.addSumCount(sumCount);
			context.write(key, totalSumCount);
		}
	}

	public static class Q4Reducer extends Reducer<Text, SumCount, Text, DoubleWritable> {

		private Map<Text, SumCount> sumCountMap = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<SumCount> values, Context context)
				throws IOException, InterruptedException {
			SumCount totalSumCount = new SumCount();

			for (SumCount sumCount : values)
				totalSumCount.addSumCount(sumCount);

			sumCountMap.put(new Text(key), totalSumCount);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, SumCount> sortedMap = sortByValues(sumCountMap);
			int counter = 0;
			for (Text key : sortedMap.keySet()) {
				if (counter++ == 10) {
					break;
				}
				SumCount entry = sortedMap.get(key);
				double mean = entry.getSum().get() / entry.getCount().get();
				context.write(key, new DoubleWritable(mean));
			}
		}
	}

	

	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

}