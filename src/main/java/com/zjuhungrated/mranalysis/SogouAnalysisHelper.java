package com.zjuhungrated.mranalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Sogou日志数据分析辅助工具类
 */
final class SogouAnalysisHelper {
    private static final String MY_JOB_NAME = "Sogou Analysis";
    private static final String MY_JOB_ROOT = "hdfs://localhost:9000";
    private static final String INPUT_PATH_STRING = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/input/sogou.test.utf8");
    private static final String OUTPUT_PATH_STRING = String.valueOf(MY_JOB_ROOT
            + "/sogouanalysis/output/");
    private static SogouAnalysisHelper instance = null;
    private static FileSystem fileSystem = null;
    private static Configuration configuration = null;
    private static Logger logger = Logger.getLogger("this.class");
    private static Path INPUT_PATH = null;
    private static Path OUTPUT_PATH = null;

    /**
     * 私有构造方法
     */
    private SogouAnalysisHelper() {
        configuration = getConfiguration();
        fileSystem = getHdfs();
    }

    /**
     * 返回单例模式的对象
     * <p>
     *
     * @return instance 单例模式的对象
     */
    static SogouAnalysisHelper getInstance() {
        if (instance == null) {
            instance = new SogouAnalysisHelper();
        }
        return instance;
    }

    /**
     * 返回系统配置信息
     * <p>
     *
     * @return configuration 系统配置信息
     */
    Configuration getConfiguration() {
        if (configuration == null) {
            configuration = new Configuration();
        }
        return configuration;
    }

    /**
     * 返回分布式文件系统对象
     * <p>
     *
     * @return fs 分布式文件系统对象
     */

    FileSystem getHdfs() {
        if (fileSystem != null) {
            return fileSystem;
        }
        FileSystem fs = null;
        try {
            fs = FileSystem.get(getConfiguration());
        } catch (IOException e) {
            logger.error("config failure:" + e.getMessage());
        }
        return fs;
    }

    /**
     * 返回任务名称
     * <p>
     *
     * @return MY_JOB_NAME 任务名称
     */
    String getJobName() {
        return MY_JOB_NAME;
    }

    /**
     * 返回输入文件路径
     * <p>
     *
     * @return INPUT_PATH 输入文件路径
     */
    Path getInputPath() {
        if (INPUT_PATH == null) {
            INPUT_PATH = new Path(INPUT_PATH_STRING);
        }
        return INPUT_PATH;
    }

    /**
     * 返回输出文件路径
     * <p>
     *
     * @return OUTPUT_PATH 输出文件路径
     */
    Path getOutputPath() {
        if (OUTPUT_PATH == null) {
            OUTPUT_PATH = new Path(OUTPUT_PATH_STRING);
        }
        return OUTPUT_PATH;
    }


//    public static void mkdir(String path) {
//        try {
//            FileSystem fs = fileSystem;
//            fs.mkdirs(new Path(path));
//            logger.info("mkdir success:" + path);
//        } catch (Exception e) {
//            logger.error("mkdir failure:" + e.getMessage());
//        }
//    }
//
//    public static void putFile(String localPath, String fileName, String hdfsPath) {
//        FileSystem fs = fileSystem;
//        try {
//            InputStream in = new FileInputStream(localPath + fileName);
//            OutputStream out = fs.create(new Path(hdfsPath + fileName));
//            IOUtils.copyBytes(in, out, 4096, true);
//            logger.info("upload success:" + fileName);
//        } catch (Exception e) {
//            logger.error("upload failure:" + e.getMessage());
//        }
//    }
//
//    public static void getFileFromHdfs(String fromPath, String fileName, String toPath) {
//        FileSystem fs = fileSystem;
//        try {
//            InputStream in = fs.open(new Path(fromPath + fileName));
//            OutputStream out = new FileOutputStream(toPath + fileName);
//            IOUtils.copyBytes(in, out, 4096, true);
//        } catch (Exception e) {
//            logger.error("getFile failure:" + e.getMessage());
//        }
//    }
//
//    public static void deleteFile(String path) {
//        FileSystem fs = fileSystem;
//        try {
//            fs.delete(new Path(path), true);
//        } catch (Exception e) {
//            logger.error("delete failure" + e.getMessage());
//        }
//    }
//
//    public static void readOutFile(String path) {
//        try {
//            InputStream inputStream = fileSystem.open(new Path(path));
//            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream, "GB2312"));
//            String line = null;
//            while ((line = bf.readLine()) != null) {
//                logger.info(line);
//            }
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//        }
//    }
}
