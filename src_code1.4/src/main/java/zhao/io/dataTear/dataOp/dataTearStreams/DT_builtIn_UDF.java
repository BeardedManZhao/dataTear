package zhao.io.dataTear.dataOp.dataTearStreams;

import zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream.*;
import zhao.io.dataTear.dataOp.dataTearStreams.localStream.*;

/**
 * 内置已经实现的自定义数据算法流的调用通行证，通过这里可以获取到对应的数据算法流
 * <p>
 * 使用者可以理解为它就是一个数据操作模式，这样调用起来会更加的亲近
 * <p>
 * 它作为 UDF数据流函数参数 会被算法库获取并提供对应的数据算法流
 */
public enum DT_builtIn_UDF {
    /**
     * 通过支持SQL的DataBase算法流构造Text的DT数据库 需要注意的是，要进行一个强转
     *
     * @see zhao.io.dataTear.dataOp.dataTearStreams.dbStream.DataBaseStream
     */
    SQLDB_TEXT,
    /**
     * HDFS中构造Text的DT目录 需要注意的是，要进行一个强转
     *
     * @see HDFSTextStream
     */
    HDFS_TEXT,
    /**
     * HDFS中构造ZIP的DT目录 需要注意的是，要进行一个强转
     *
     * @see HDFSZIPStream
     */
    HDFS_ZIP,
    /**
     * HDFS中构造ZIP的DT目录 需要注意的是，要进行一个强转
     *
     * @see HDFSGZIPStream
     */
    HDFS_GZIP,
    /**
     * HDFS中构造BZIP2的DT目录 需要注意的是，要进行一个强转
     *
     * @see HDFSBZIP2Stream
     */
    HDFS_BZIP2,
    /**
     * HDFS中构造BZIP2的DT目录 需要注意的是，要进行一个强转
     *
     * @see HDFSSnappyStream
     */
    HDFS_SNAPPY,
    /**
     * 本地文件系统中构造Text的DT目录
     *
     * @see LocalBufferStream
     */
    LOCAL_TEXT,
    /**
     * 本地文件系统中构造ZIP处理的的DT目录
     *
     * @see LocalZIPStream
     */
    LOCAL_ZIP,
    /**
     * 本地文件系统中构造GZIP处理的DT目录
     *
     * @see LocalGZIPStream
     */
    LOCAL_GZIP,
    /**
     * 本地文件系统中构造BZIP2处理的DT目录
     *
     * @see LocalBZIP2Stream
     */
    LOCAL_BZIP2,
    /**
     * 本地文件系统中构造Snappy处理的DT目录
     *
     * @see LocalSnappyStream
     */
    LOCAL_SNAPPY
}
