package zhao.runCli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import zhao.io.dataTear.config.ConfigBase;
import zhao.io.dataTear.config.RunInitClass;
import zhao.io.ex.CommandParsingException;
import zhao.runCli.directive.Execute;
import zhao.runCli.directive.ZHAOPut;
import zhao.runCli.directive.ZHAOSet;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Scanner;

/**
 * @author 赵凌宇
 * 内置的客户端程序，客户端中使用本框架设计了一套文件读写系统，使用交互式命令操作，这是除了框架的第二种使用方法
 */
public class MAINCli {
    public static FileSystem fileSystem;
    static Scanner scanner = new Scanner(System.in);
    static Configuration configuration;
    static RunInitClass.R r = new RunInitClass.R(ConfigBase.JarsPath);

    public static void main(String[] args) {
        if (args.length > 1) {
            if (r.runAllClass(("zhao.run.dt.plug.timeOut.RUN," + (args.length > 3 ? args[3] : "")).split(","))) {
                configuration = new Configuration();
                if (new File(ConfigBase.log4j).exists()) {
                    PropertyConfigurator.configure(ConfigBase.log4j);
                } else {
                    BasicConfigurator.configure();
                }
                if (args[0].equalsIgnoreCase("y")) {
                    initHDFS(args[1], args[2]);
                }
                Execute.addExecute(new ZHAOPut(fileSystem));
                Execute.addExecute(new ZHAOSet());

                boolean stat = true;
                while (stat) {
                    System.out.print("Liming >>> ");
                    String[] s = scanner.nextLine().split("\\s+");
                    if (s[0].equalsIgnoreCase("exit")) {
                        stat = false;
                        if (fileSystem != null) {
                            try {
                                fileSystem.close();
                            } catch (IOException ignored) {
                            }
                        }
                        for (String command : Execute.ExecuteS.keySet()) {
                            try {
                                Execute.ExecuteS.get(command).close();
                            } catch (Exception ignored) {
                            }
                        }
                    } else {
                        Execute execute = Execute.ExecuteS.get(s[0]);
                        if (execute != null) {
                            try {
                                Objects.requireNonNull(execute).open(s);
                                if (execute.run()) {
                                    System.out.println("* >>> ok!!!");
                                } else {
                                    System.err.println("* >>> 执行失败！");
                                }
                            } catch (CommandParsingException | NullPointerException n) {
                                System.err.println("* >>> 执行错误，原因如下：：");
                                n.printStackTrace();
                            } finally {
                                try {
                                    execute.close();
                                } catch (NullPointerException ignored) {
                                }
                            }
                            try {
                                Thread.sleep(124);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.err.println("* >>> 命令" + s[0] + "的执行类没有找到！");
                        }
                    }
                }
            } else {
                System.out.println("* >>> 您的启动初始化有错误，请检查初始化插件。");
            }
        } else {
            System.out.println("* >>> 您的启动设置有错误！需要参数：【是否使用HDFS文件系统】 【HDFS ip】 【HDFS port】【插件类路径】");
        }
    }

    /**
     * 初始化HDFS文件系统对象
     *
     * @param IP   HDFS主节点IP
     * @param port HDFS主节点端口
     */
    public static void initHDFS(String IP, String port) {
        configuration.set("fs.default.name", "hdfs://" + IP + ":" + port);
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Execute.addExecute(new ZHAOPut(fileSystem));
    }
}
