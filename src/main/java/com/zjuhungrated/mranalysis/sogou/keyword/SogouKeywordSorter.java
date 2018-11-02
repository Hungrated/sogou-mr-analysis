package com.zjuhungrated.mranalysis.sogou.keyword;

import com.zjuhungrated.mranalysis.common.SogouAnalysisHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Sogou搜索日志关键词分析MapReduce排序程序类
 */

public class SogouKeywordSorter {

    private static final String MY_JOB_NAME = "Sogou Keyword Sorter";
    private static final String EXPORT_PATH = "out/result";

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
        job.setJarByClass(SogouKeywordSorter.class);

        // 指定本次MR的Mapper Combiner和Reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // 指定本次MR任务Map阶段的输出K V类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 指定本次MR任务最终输出K V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path in = helper.getPath(inputPath);
        Path out = helper.getPath(outputPath);

        // 指定本次MR任务的输入输出文件路径
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setSortComparatorClass(MyComparator.class);

        // 删除上次运行结果（若有） 以保证本次结果正常输出
        helper.deleteFileIfExists(outputPath);

        if (job.waitForCompletion(false)) {
            System.out.println("Sort complete, now get result");
            helper.getFileFromHdfs(outputPath, "part-r-00000", EXPORT_PATH);
            helper.printFile(outputPath, "part-r-00000", 30);
            System.exit(0);
        } else {
            System.exit(1);
        }

    }

    /**
     * 自定义Comparator类 实现倒序排序
     */
    public static class MyComparator extends Comparator {
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    /**
     * SortMapper
     * <p>
     * 对文档进行Map操作类
     */
    public static class SortMapper extends Mapper<Object, Text, LongWritable, Text> {

        /**
         * map
         * <p>
         * 将输入文档Key Value互换并输出给Reducer
         *
         * @param key     关键词
         * @param value   待处理文本
         * @param context 将输出的上下文
         * @throws IOException          当读文件错误抛出该异常
         * @throws InterruptedException 当被中断时抛出该异常
         */
        public void map(Object key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            context.write(new LongWritable(Integer.parseInt(split[1])), new Text(split[0]));
        }
    }

    /**
     * SortReducer
     * <p>
     * 对文档进行Reduce操作类
     */
    public static class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        /**
         * reduce
         * <p>
         * 处理并输出排序结果
         *
         * @param key     关键词
         * @param values  待处理文本
         * @param context 将输出的上下文
         * @throws IOException          当读文件错误抛出该异常
         * @throws InterruptedException 当被中断时抛出该异常
         */
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(text, key);
            }
        }
    }

}
