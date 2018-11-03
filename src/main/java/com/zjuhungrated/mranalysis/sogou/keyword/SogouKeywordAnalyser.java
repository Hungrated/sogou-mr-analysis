package com.zjuhungrated.mranalysis.sogou.keyword;

/**
 * Sogou搜索日志关键词分析MapReduce主程序类
 */
public class SogouKeywordAnalyser {

    private static final String MY_JOB_ROOT = "hdfs://localhost:9000";
    private static final String INPUT_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/input/sogou.full.utf8.ext");
    private static final String OUTPUT_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/output/");

    public static void main(String[] args) throws Exception {
        SogouKeywordCounter.run(INPUT_PATH, OUTPUT_PATH);
    }

}
