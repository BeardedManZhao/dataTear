package zhao.runCli.directive;

import org.apache.hadoop.fs.FileSystem;
import zhao.io.dataTear.config.ConfigBase;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearRW.DTMaster;
import zhao.io.dataTear.dataOp.dataTearRW.DTMasterBuilder;
import zhao.io.dataTear.dataOp.dataTearRW.RW;
import zhao.io.dataTear.dataOp.dataTearStreams.DTRead;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;
import zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream.*;
import zhao.io.ex.CommandParsingException;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * 向HDFS中上传一个文件
 */
public class ZHAOPut implements Execute {
    private final FileSystem fileSystem;
    /**
     * 数据执行组件
     */
    private DTMaster DTMaster;

    /**
     * 本执行类执行命令 put 【源文件类型】【源文件路径】【输出DataTear目录】【输入数据算法】【输出数据算法】【输入分割正则】【输出分割符号】【DT数据块数】
     *
     * @param fileSystem HDFS文件对象，put命令可能会涉及到HDFS的操作，需要使用fileSystem
     */
    public ZHAOPut(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * @param StreamCommType 需要获取的流集成组件模式 默认是Text
     * @return 您需要的组件
     */
    private DT_StreamBase getHDFSRWStream(String StreamCommType) {
        switch (StreamCommType) {
            case "TEXT":
                return ((HDFSTextStream) Objects.requireNonNull(RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_TEXT))).setFileSystem(fileSystem);
            case "ZIP":
                return ((HDFSZIPStream) Objects.requireNonNull(RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_ZIP))).setFileSystem(fileSystem);
            case "GZIP":
                return ((HDFSGZIPStream) Objects.requireNonNull(RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP))).setFileSystem(fileSystem);
            case "BZIP2":
                return ((HDFSBZIP2Stream) Objects.requireNonNull(RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_BZIP2))).setFileSystem(fileSystem);
            case "SNAPPY":
                return ((HDFSSnappyStream) Objects.requireNonNull(RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_SNAPPY))).setFileSystem(fileSystem);
            default:
                System.err.println("* >>> 您设置的输出模式不存在，因此使用默认的HDFS-Text数据集成流组件。");
                return Objects.requireNonNull(RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_TEXT));
        }
    }

    private DT_StreamBase getLOCALRWStream(String StreamCommType) {
        switch (StreamCommType) {
            case "TEXT":
                return RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT);
            case "ZIP":
                return RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_ZIP);
            case "GZIP":
                return RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP);
            case "BZIP2":
                return RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_BZIP2);
            case "SNAPPY":
                return RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_SNAPPY);
            default:
                System.err.println("* >>> 您设置的输出模式不存在，因此使用默认的Local-Text数据集成流组件。");
                return RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT);
        }
    }

    @Override
    public String getHelp() {
        return "put 【源文件类型】【源文件路径】【输出DT目录】【输入数据算法】【输出数据算法】【输入分割正则】【输出分割符号】【DT数据块数】\n" +
                "参数详解：注意 参数带有'(*)'的话，代表是必填参数！！\n" +
                "\t【源文件类型(*)】  NODT(非DT文件) DT(DT文件的NameManager) \n" +
                "\t【源文件路径(*)】  需要被DT构建的文件路径（是文件路径哦）\n" +
                "\t【输出DT目录(*)】 将哪个目录作为源文件的DT的构造, 相当于是输出目录路径。\n" +
                "\t【输入数据算法(*)】 使用哪一种算法读取数据，需要大写哦！\n" +
                "\t【输出数据算法(*)】 使用哪一种算法输出数据，需要大写哦！\n" +
                "\t【输入分割正则】    使用正则表达式/字符串，将输入的数据进行拆分,默认是按照非单词拆分。\n" +
                "\t【输出分割符号】    使用字符串作为最终输出的数据的列分隔符，默认是按照逗号拆分。\n" +
                "\t【DT数据块数】    被构造出来的DT目录中包含多少个数据块，默认是3个。";
    }

    @Override
    public String GETCommand_NOE() {
        return "put";
    }

    @Override
    public boolean open(String[] args) {
        int TL = args.length;
        if (TL < 6) {
            throw new CommandParsingException("您的put格式命令不正确，请改正为：" + getHelp());
        } else {
            try {
                DTMaster = new DTMasterBuilder(OUT_FilePath -> args[3].contains("hdfs://") || args[3].contains("HDFS://") ?
                        getHDFSRWStream(args[5]).writeStream(OUT_FilePath) :
                        getLOCALRWStream(args[5]).writeStream(OUT_FilePath)
                ).ReadFormat(DataSourceFormat.UDT)
                        .WriterFormat(DataOutputFormat.UDT)
                        .setOUT_FilePath(args[3])
                        .setCharset(ConfigBase.Outcharset())
                        .setReader(
                                args[1].equalsIgnoreCase("DT") ? new DTRead(
                                        In_FilePath -> In_FilePath.contains("hdfs://") || In_FilePath.contains("HDFS://") ? getHDFSRWStream(args[4]).readStream(In_FilePath) : getLOCALRWStream(args[4]).readStream(In_FilePath)).setPrimaryCharacteristic(data -> true).setIn_FilePath(args[2]
                                ) : (args[2].contains("hdfs://") || args[2].contains("HDFS://") ? getHDFSRWStream(args[4]).readStream(args[2]) : getLOCALRWStream(args[4]).readStream(args[2]).setIn_File(new File(args[2])))
                        )
                        .setSplitrex(TL >= 7 ? args[6] : "\\s+")
                        .setOutSplit(TL >= 8 ? args[7] : ",")
                        .setFragmentationNum(TL >= 9 ? Integer.parseInt(args[8]) : 3).create();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                return DTMaster.openStream();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    @Override
    public boolean run() {
        return DTMaster.op_Data();
    }

    @Override
    public boolean close() {
        if (DTMaster != null) {
            return DTMaster.closeStream();
        }
        return false;
    }
}
