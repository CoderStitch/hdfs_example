package com.stitch.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 创建目录
        fs.mkdirs(new Path("/1108/daxian/banzhang"));
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("e:/code_lwh/hadoop/example/banzhang.txt"), new Path("/banzhang.txt"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/banzhang.txt"),
                new Path("e:/code_lwh/hadoop/example/banhua.txt"), true);
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 执行删除
        fs.delete(new Path("/1108/"), true);
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://150.158.174.69:9000"),
                configuration, "ubuntu");
        // 修改文件名称
        fs.rename(new Path("/banzhang.txt"), new Path("/banzhang2.txt"));
        // 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles =
                fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations =
                    status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------------------");
        }
        // 3 关闭资源
        fs.close();
    }

    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
        // 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 判断是文件试试文件夹

    }

}
