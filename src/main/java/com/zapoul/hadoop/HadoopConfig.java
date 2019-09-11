package com.zapoul.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/10
 * Time:16:22
 */
@Configuration
@ConditionalOnProperty(name="hadoop.name-node")
public class HadoopConfig {

    @Value("${hadoop.name-node}")
    private String nameNode;

    @Bean("fileSystem")
    public FileSystem createFs(){
        //读取配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        //conf.set("fs.defalutFS", "hdfs://192.168.169.128:9000");
        //设置副本数，默认1
        conf.set("dfs.replication", "1");
        //指定访问hdfs的客户端身份
        //fs = FileSystem.get(new URI("hdfs://192.168.169.128:9000/"), conf, "root");
        // 文件系统
        FileSystem fs = null;
        // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
        try {
            URI uri = new URI(nameNode.trim());
            fs = FileSystem.get(uri,conf);
        } catch (Exception e) {
            System.out.println(e);
        }
        return  fs;
    }



}
