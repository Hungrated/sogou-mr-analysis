package com.zjuhungrated.mranalysis.sogou.url;

/**
 * Sogou搜索日志URL分析MapReduce主程序类
 */
public class SogouUrlAnalyser {

    private static final String MY_JOB_ROOT = "hdfs://localhost:9000";
    private static final String INPUT_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/input/sogou.full.utf8");
    private static final String OUTPUT_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/output2/");


    public static void main(String[] args) throws Exception {
        SogouUrlCounter.run(INPUT_PATH, OUTPUT_PATH);
    }

}
