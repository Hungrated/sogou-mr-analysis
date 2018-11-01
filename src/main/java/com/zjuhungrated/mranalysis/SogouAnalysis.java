package com.zjuhungrated.mranalysis;

/**
 * Sogou日志数据分析MapReduce主程序类
 */
public class SogouAnalysis {

    private static final String MY_JOB_ROOT = "hdfs://localhost:9000";
    private static final String INPUT_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/input/sogou.test.utf8");
    private static final String OUTPUT_PATH = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/output/");


    public static void main(String[] args) throws Exception {
        SogouAnalysisCounter.run(INPUT_PATH, OUTPUT_PATH);
    }

}
