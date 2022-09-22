package zhao.io.dataTear.config;

import java.util.HashMap;

/**
 * 配置信息库 Configuration repository
 */
public class ConfigBase {

    /**
     * 配置文件路径 configuration file path
     */
    public static final String confPath = "conf";
    /**
     * 插件依赖路径 plugin dependency path
     */
    public static final String JarsPath = "lib/jars";
    /**
     * log4j日志文件 log4j log file path
     */
    public static final String log4j = confPath + "/log4j.properties";
    public static HashMap<String, String> conf = new HashMap<>();

    /**
     * 客户端使用数据输出编码 Client uses data output encoding
     *
     * @return 数据输出使用的编码集 encoding set used for data output
     */
    public static String Outcharset() {
        return conf.getOrDefault("Out.charset", "utf-8");
    }

    /**
     * 客户端使用数据输入编码 Client uses data input encoding
     *
     * @return 数据输入使用的编码集，这里目前在框架中不会被调用，框架会自动获取文件字符编码并解析，拓展框架时可能会使用到。
     * <p>
     * The encoding set used for data input, which will not be called in the framework at present. The framework will automatically obtain the character encoding of the file and parse it. It may be used when expanding the framework.
     */
    public static String Incharset() {
        return conf.getOrDefault("In.charset", "utf-8");
    }
}
