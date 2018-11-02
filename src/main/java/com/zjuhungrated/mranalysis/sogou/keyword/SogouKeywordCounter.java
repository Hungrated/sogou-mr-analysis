package com.zjuhungrated.mranalysis.sogou.keyword;

import com.zjuhungrated.mranalysis.common.SogouAnalysisHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Sogou搜索日志关键词分析MapReduce计数程序类
 */

public class SogouKeywordCounter {

    private static final String MY_JOB_NAME = "Sogou Keyword Counter";
    private static final String MY_JOB_ROOT = "hdfs://localhost:9000";
    private static final String TEMP_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/temp/");
    private static final String TEMP_FILE_PATH = String.valueOf(TEMP_PATH
            + "part-r-00000");

    /**
     * 运行本次MapReduce任务
     * <p>
     *
     * @param inputPath  输入文件路径
     * @param outputPath 输出文件路径
     * @throws Exception 当出现异常时抛出
     */
    @SuppressWarnings("deprecation")
    public static void run(String inputPath, String outputPath) throws Exception {

        // helper封装基本操作
        SogouAnalysisHelper helper = SogouAnalysisHelper.getInstance();

        // Job封装本次MapReduce相关信息
        Job job = new Job(helper.getConfiguration(), MY_JOB_NAME);

        // 指定本次MR任务jar包运行主类
        job.setJarByClass(SogouKeywordCounter.class);

        // 指定本次MR的Mapper Combiner和Reducer
        job.setMapperClass(LineParserMapper.class);
        job.setCombinerClass(LongSumSortReducer.class);
        job.setReducerClass(LongSumSortReducer.class);

        // 指定本次MR任务Map阶段的输出K V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定本次MR任务最终输出K V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path in = helper.getPath(inputPath);
        Path out = helper.getPath(TEMP_PATH);

        // 指定本次MR任务的输入输出文件路径
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // 删除上次运行结果（若有） 以保证本次结果正常输出
        helper.deleteFileIfExists(TEMP_PATH);

        if (job.waitForCompletion(false)) {
            System.out.println("Count complete, now sort");
            SogouKeywordSorter.run(TEMP_FILE_PATH, outputPath);
        }
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
     * LongSumSortReducer
     * <p>
     * 对Map结果进行Reduce操作类
     */

    public static class LongSumSortReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

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

}
