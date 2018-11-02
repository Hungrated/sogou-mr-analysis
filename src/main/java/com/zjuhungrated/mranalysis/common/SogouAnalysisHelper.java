package com.zjuhungrated.mranalysis.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Sogou搜索日志分析辅助工具类
 */
public final class SogouAnalysisHelper {

    private static SogouAnalysisHelper instance = null;
    private static FileSystem fileSystem = null;
    private static Configuration configuration = null;

    private static Logger logger = Logger.getLogger("this.class");

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
    public static SogouAnalysisHelper getInstance() {
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
    public Configuration getConfiguration() {
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

    public FileSystem getHdfs() {
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
     * 返回文件路径
     * <p>
     *
     * @return path 文件路径对象
     */
    public Path getPath(String path) {
        return new Path(path);
    }

    /**
     * 将HDFS上的文件保存到本地
     * <p>
     *
     * @param fromPath 源路径
     * @param fileName 文件名
     * @param toPath   目标文件路径（含文件名）
     */
    public void getFileFromHdfs(String fromPath, String fileName, String toPath) {
        FileSystem fs = fileSystem;
        try {
            InputStream in = fs.open(new Path(fromPath + fileName));
            OutputStream out = new FileOutputStream(toPath);
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (Exception e) {
            logger.error("getFileFromHdfs failure: " + e.getMessage());
        }
    }

    /**
     * 删除文件（若该文件存在）
     * <p>
     *
     * @param path 文件路径
     */
    public void deleteFileIfExists(String path) {
        FileSystem fs = fileSystem;
        Path out = new Path(path);
        try {
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
        } catch (Exception e) {
            logger.error("delete failure" + e.getMessage());
        }
    }

    /**
     * 将文本文件内容打印到控制台
     *
     * @param path  文件路径
     * @param lines 打印行数
     */
    public void printFile(String path, String fileName, int lines) {
        try {
            InputStream inputStream = fileSystem.open(new Path(path + fileName));
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            String line;
            int count = 0;
            while ((line = bf.readLine()) != null && count < lines) {
                logger.info(line);
                count = count + 1;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
