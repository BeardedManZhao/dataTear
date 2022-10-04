package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.IOException;

public class HDFSReader extends Reader {

    FSDataInputStream fsDataInputStream;
    Path In_path;
    String In_Pathstr = "";

    public HDFSReader(FileSystem fileSystem, Path in_path, String in_Pathstr) {
        try {
            this.fsDataInputStream = fileSystem.open(in_path);
            In_path = in_path;
        } catch (IOException e) {
            logger.error("组件：" + this.getClass().getName() + " 启动数据流时出现异常！目标数据：" + in_Pathstr + ",错误原因：" + e);
            e.printStackTrace(System.err);
        }
    }

    /**
     * 开始建造HDFS输出组件 Start building HDFS output components
     *
     * @return 建造者对象
     */
    public static HDFSReaderBuilder builder() {
        return new HDFSReaderBuilder();
    }

    public FSDataInputStream getFsDataInputStream() {
        return fsDataInputStream;
    }

    public Path getIn_path() {
        return In_path;
    }

    public String getIn_Pathstr() {
        return In_Pathstr;
    }

    @Override
    public HDFSReader toTobject() {
        return this;
    }

    @Override
    public boolean closeStream() throws IOException {
        close();
        return super.closeStream();
    }

    public int available() throws IOException {
        return fsDataInputStream.available();
    }

    public void close() {
        try {
            fsDataInputStream.close();
        } catch (IOException e) {
            logger.warn("An exception occurred in HDFS close", e);
        }
    }
}
