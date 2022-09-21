package zhao.runCli.directive;

import java.util.TreeMap;

/**
 * 命令执行类的接口，客户端命令拓展接口
 */
public interface Execute {
    /**
     * 命令执行者列表 key一般来说是命令关键字  value 就是命令执行者
     */
    TreeMap<String, Execute> ExecuteS = new TreeMap<>();

    /**
     * 向客户端添加一个命令执行模块，为客户端拓展功能，外部的命令执行类需要使用本方法将自己添加进执行库
     *
     * @param executeClass 命令执行类，是命令的具体执行者，被拓展的命令功能
     */
    static void addExecute(Execute executeClass) {
        Execute.ExecuteS.put(executeClass.GETCommand_NOE(), executeClass);
    }

    /**
     * 执行命令的正确格式，会被异常调用
     *
     * @return 本执行类所需的命令格式
     */
    public String getHelp();

    /**
     * 获取该类的启动命令，通常用来解析命令，并调用对应的执行类
     *
     * @return 本执行类的执行识别命令
     */
    String GETCommand_NOE();

    /**
     * 初始化本执行类，一般会用来打开执行类需要的连接或数据流
     *
     * @param args 客户端执行命令参数
     * @return 是否初始化成功
     */
    boolean open(String[] args);

    /**
     * 执行本类的功能
     *
     * @return 是否成功执行本类
     */
    boolean run();

    /**
     * 关闭本类的功能，相当于是将本类关机
     *
     * @return 是否成功关闭
     */
    boolean close();
}
