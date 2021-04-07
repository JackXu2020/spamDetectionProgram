package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.InputStream;
import java.net.URI;

public class HdfsOps {
    String url;
    public HdfsOps(String url) {
        //super();
        this.url = url;
    }

    FileSystem getFileSystem() throws Exception {
        URI uri = new URI(url);
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        return fileSystem;
    }

    public void uploadFile(String srcPath, String destPath) throws Exception {
        FileSystem fileSystem = getFileSystem();
        Path src = new Path(srcPath);
        Path dest = new Path(destPath);
        fileSystem.copyFromLocalFile(src, dest);
    }

    public void createFile(String file, String content) throws Exception {
        byte[] buff = content.getBytes();
        FileSystem fileSystem = getFileSystem();
        FSDataOutputStream outputStream = fileSystem.create(new Path(file));
        outputStream.write(buff, 0, buff.length);
        outputStream.close();
    }

    public void createDir(String dir) throws Exception {
        FileSystem fileSystem = getFileSystem();
        fileSystem.mkdirs(new Path(dir));
    }

    public void fileRename(String oldPath, String newPath) throws Exception {
        FileSystem fileSystem = getFileSystem();
        boolean isRename = fileSystem.rename(new Path(oldPath), new Path(newPath));
        String result = isRename ? "successful" : "failed";
        System.out.println("Rename file is " + result);
    }

    public void deleteFile(String path) throws Exception {
        FileSystem fileSystem = getFileSystem();
        boolean isDeleted = fileSystem.delete(new Path(path), true);
        System.out.println("file is deleted?" + isDeleted);
    }

    public void readFile(String fileName) throws Exception {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path(fileName));
        IOUtils.copyBytes(inputStream, System.out, 1024, false);
        IOUtils.closeStream(inputStream);
    }

    public void fileLastModified(String path) throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = fs.getFileStatus(new Path(path));
        long time = fileStatus.getModificationTime();
        System.out.println("last modified time is" + time);
    }

    public void fileLocation(String fileName) throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = fs.getFileStatus(new Path(fileName));
        BlockLocation[] locations = fs.getFileBlockLocations(fileStatus,
                0, fileStatus.getLen());
        for (int i=0; i<locations.length;i++) {
            String[] hosts = locations[i].getHosts();
            System.out.println("block " + i + " location host is " + hosts[0]);
        }
    }

    public void nodeList() throws Exception {
        FileSystem fs = getFileSystem();
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        DatanodeInfo[] datanodeInfos = dfs.getDataNodeStats();
        for (int i=0; i<datanodeInfos.length; i++) {
            System.out.println("DataNode " + i + " Name " + datanodeInfos[i].getHostName());
        }
    }

    public void readCompressionFile(String fileName) throws Exception {
        FileSystem fs = getFileSystem();
        Path filePath = new Path(fileName);
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        for (Class cls: CompressionCodecFactory.getCodecClasses(new Configuration())) {
            System.out.println(cls);
        }

        CompressionCodec codec = factory.getCodec(filePath);
        if (codec==null) {
            System.err.println("the file has no codec.");
        } else {
            InputStream in = codec.createInputStream(fs.open(filePath));
            IOUtils.copyBytes(in, System.out, fs.getConf());
            IOUtils.closeStream(in);
        }
    }

    public void readCompressionFileWithCodecPool(String fileName) throws Exception {
        FileSystem fs = getFileSystem();
        CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class,
                fs.getConf());
        Compressor compressor = CodecPool.getCompressor(codec);
        InputStream in = codec.createInputStream(fs.open(new Path(fileName)));
        IOUtils.copyBytes(in, System.out, fs.getConf());
        IOUtils.closeStream(in);
        CodecPool.returnCompressor(compressor);
    }

    public void readSequenceFile(String fileName) throws Exception {
        Path filePath = new Path(fileName);
        Configuration conf = new Configuration();
        FileSystem fs = getFileSystem();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, filePath, conf);
            Writable key = (Writable) ReflectionUtils.
                    newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.
                    newInstance(reader.getValueClass(), conf);
            reader.sync(1956);
            while (reader.next(key, value)) {
                long position = reader.getPosition();
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n",
                        position, syncSeen, key, value);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    public static void main(String[] args) throws Exception {
        HdfsOps hdfsOps = new HdfsOps("hdfs://localhost:9000");
        hdfsOps.fileRename("/test", "/file1");

    }



















}
