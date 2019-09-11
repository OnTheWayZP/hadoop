package com.zapoul.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = HadoopApplicationTests.class)
public class HadoopApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Value("${hadoop.name-node}")
    private String nameNode;

    @Value("${hadoop.namespace}")
    private String filePath;

    @Test
    public void init() throws URISyntaxException, IOException {
        //指定刚才解压缩hadoop文件地址
        System.setProperty("hadoop.home.dir", "H:\\hadoop-2.7.2");
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNode), configuration);
        //2.执行操作 创建hdfs文件夹
        Path path = new Path(filePath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        //关闭资源
        fs.close();
        System.out.println("结束！");
    }

    @Test
    public void upload() throws URISyntaxException, IOException {
        System.setProperty("hadoop.home.dir", "H:\\hadoop-2.7.2");
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNode), configuration);
        //2.执行操作 上传文件
        fs.copyFromLocalFile(false,true, new Path("C:\\Users\\New\\Desktop\\test.txt"), new Path(filePath));
        //关闭资源
        fs.close();
        System.out.println("结束！");
    }

    @Test
    public void download() throws URISyntaxException, IOException {
        System.setProperty("hadoop.home.dir", "H:\\hadoop-2.7.2");
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNode), configuration);
        //2.执行操作 下载文件
        fs.copyToLocalFile(false, new Path(filePath+"/test.txt"), new Path("D:\\"), true);
        //关闭资源
        fs.close();
        System.out.println("结束！");
    }

}
