package com.zjuhungrated.mranalysis;

import com.zjuhungrated.mranalysis.utils.SogouAnalysisHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Sogou日志数据分析MapReduce主程序类
 */

public class SogouAnalysis {

    public static void main(String[] args) throws Exception {

        SogouAnalysisHelper helper = SogouAnalysisHelper.getInstance();

        FileSystem fs = helper.getHdfs();

        // Job封装本次MapReduce相关信息
        Job job = Job.getInstance(helper.getConfiguration(), helper.getJobName());

        // 指定本次MR任务jar包运行主类
        job.setJarByClass(SogouAnalysis.class);

        // 指定本次MR的Mapper Combiner和Reducer
        job.setMapperClass(LineParserMapper.class);
        job.setCombinerClass(IntSumSortReducer.class);
        job.setReducerClass(IntSumSortReducer.class);
        job.setSortComparatorClass(LongWritableDecreasingComparator.class);

        // 指定本次MR任务Map阶段的输出K V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定本次MR任务最终输出K V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path in = helper.getInputPath();
        Path out = helper.getOutputPath();

        // 指定本次MR任务的输入输出文件路径
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // 删除上次运行结果（若有） 以保证本次结果正常输出
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * LineParserMapper
     * <p>
     * 对文档进行Map操作类
     */

    public static class LineParserMapper extends Mapper<Object, Text, Text, LongWritable> {

        /**
         * map
         * <p>
         * 重写map方法，将输入文档做拆词处理并输出给Reducer
         *
         * @param key     关键词
         * @param value   待处理文本
         * @param context 将输出的上下文
         * @throws IOException          当读文件错误抛出该异常
         * @throws InterruptedException 当被中断时抛出该异常
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            String keyWord = words[2] == null ? "" : words[2].trim();
            context.write(new Text(keyWord), new LongWritable(1));
        }
    }

    /**
     * IntSumSortReducer
     * <p>
     * 对Map结果进行Reduce操作类
     */

    public static class IntSumSortReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * reduce
         * <p>
         * 重写reduce方法，计算每个词出现次数总和
         *
         * @param key     关键词
         * @param values  文本数组
         * @param context 将输出的上下文
         * @throws IOException          当读文件错误抛出该异常
         * @throws InterruptedException 当被中断时抛出该异常
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static class LongWritableDecreasingComparator extends
            LongWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

}
