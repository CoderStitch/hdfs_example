package com.stitch.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 创建目录
        fs.mkdirs(new Path("/1107/daxian/banzhang"));
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
        fs.delete(new Path("/ubuntu/output/wordcount/out"), true);
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

    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
        // 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 判断是文件试试文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("file:"+fileStatus.getPath().getName());
            }else {
                System.out.println("dir:"+fileStatus.getPath().getName());
            }
        }
        // 关闭资源
        fs.close();
    }

    // 下面代码位使用IO流操作HDFS
    @Test
    public void putFileToHDFS() throws IOException,InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/code_lwh/hadoop/hdfs_example/banhua1.txt"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banhua1.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua1.txt"));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/code_lwh/hadoop/hdfs_example/banhua2.txt"));
        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 需求：分块读取 HDFS 上的大文件，比如根目录下的/hadoop-2.10.1.tar.gz
    // 由于没有上传代码，所以就不测试了，下面分别是下载第一块和第二块
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new
                URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.1.tar.gz"));
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.10.1.tar.gz.part1"));
        // 4 流的拷贝
        byte[] buf = new byte[1024];
        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }
        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://150.158.174.69:9000"), configuration, "ubuntu");
        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.1.tar.gz"));
        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.10.1.tar.gz.part2"));
        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
