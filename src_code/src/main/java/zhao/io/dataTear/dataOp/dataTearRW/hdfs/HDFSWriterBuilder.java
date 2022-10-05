package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.atzhaoPublic.Builder;
import zhao.io.dataTear.atzhaoPublic.Priority;

import java.io.IOException;

/**
 * HDFS数据输出组件建造者  HDFS data output component builder
 */
public class HDFSWriterBuilder implements Builder<HDFSWriter> {
    private final Configuration configuration = new Configuration();
    private String pathString;
    private Path pathObject;
    private FileSystem fileSystem;

    /**
     * 定位HDFS信息
     * 如果没有进行过过FileSystem的设置，本设置将会生效，也就是说 它是一种备用方法，它将会通过IP与端口找到HDFS集群
     * <p>
     * Locate HDFS information
     * If FileSystem has not been set, this setting will take effect, that is to say, it is an alternate method, it will find the HDFS cluster by IP and port
     *
     * @param IP   HDFS集群通讯地址 一般是主NameNode信息
     *             <p>
     *             cluster communication address is generally the main NameNode information
     * @param port 通讯端口
     *             communication port
     * @return 链
     */
    @Priority("2")
    public HDFSWriterBuilder setIP_port(String IP, String port) {
        configuration.set("fs.default.name", "hdfs://" + IP + ":" + port);
        return this;
    }

    /**
     * 定制更多配置信息
     * 如果没有进行过过FileSystem的设置，本设置将会生效，也就是说 它是一种备用方法
     * <p>
     * Customize more configuration information If no FileSystem settings have been made, this setting will take effect, which means it is an alternate method
     *
     * @param key   HDFS配置名称
     * @param value 配置参数
     * @return 链
     */
    @Priority("2")
    public HDFSWriterBuilder setKV(String key, String value) {
        configuration.set(key, value);
        return this;
    }

    /**
     * 定位输出路径 该方法不一定会被调用，因为针对文件输出路径的设置由DataTear去实现
     * <p>
     * Locate the output path This method is not necessarily called, because the settings for the file output path are implemented by DataTear
     *
     * @param pathString 设置文件输出路径 set file path
     * @return 链
     */
    @Priority("3")
    public HDFSWriterBuilder setPathString(String pathString) {
        this.pathString = pathString;
        return this;
    }

    /**
     * 定位输出路径 该方法不一定会被调用，因为针对文件输出路径的设置由DataTear去实现
     * <p>
     * Locate the output path This method is not necessarily called, because the settings for the file output path are implemented by DataTear
     *
     * @param pathObject 设置文件路径对象 set file path
     * @return 链
     */
    @Priority("2")
    public HDFSWriterBuilder setPathObject(Path pathObject) {
        this.pathObject = pathObject;
        return this;
    }

    /**
     * 直接通过FileSystem对象构建输出组件，这个是非常推荐的方法
     * <p>
     * Build the output component directly from the File System object, this is a very recommended method
     *
     * @param fileSystem HDFS file System
     * @return 链
     */
    @Priority("1")
    public HDFSWriterBuilder setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        return this;
    }

    @Override
    public HDFSWriter create() {
        try {
            if (pathObject == null) pathObject = new Path(pathString);
            return new HDFSWriter(fileSystem, pathObject);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
