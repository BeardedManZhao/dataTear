package zhao.io.dataTear.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * <h2>introduce</h2>
 * <hr>
 * <h3>中文</h3>
 * 根据properties中配置的路径把jar和配置文件加载到classpath中。<br>
 * 此工具类加载类时使用的是SystemClassLoader，如有需要对加载类进行校验，请另外实现自己的加载器
 * <h3>English</h3>
 * Load the jar and configuration file into the classpath according to the path configured in properties.
 * This tool class uses SystemClassLoader when loading classes. If you need to verify the loaded class, please implement your own loader.
 */
public class ExtClasspathLoader {
//    private static final Logger LOG = LoggerFactory.getLogger(ExtClasspathLoader.class);

    private static final String JAR_SUFFIX = ".jar";
    private static final String ZIP_SUFFIX = ".zip";

    /**
     * URLClassLoader的addURL方法
     */
    private static final Method addURL = initAddMethod();

    /**
     * Application Classloader
     */
    private static final URLClassLoader classloader = (URLClassLoader) ClassLoader.getSystemClassLoader();

    /**
     * 初始化addUrl 方法.
     *
     * @return 可访问addUrl方法的Method对象
     */
    private static Method initAddMethod() {
        try {
            Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            add.setAccessible(true);
            return add;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过filepath加载文件到classpath。
     *
     * @param file 文件路径
     * @throws Exception 文件url解析异常
     */
    private static void addURL(File file) throws Exception {
        addURL.invoke(classloader, file.toURI().toURL());
    }

    /**
     * load Resource by Dir
     *
     * @param file dir
     * @throws IOException 加载类插件文件异常
     */
    public static void loadResource(File file) throws Exception {
        // 资源文件只加载路径
        System.out.println("load Resource of dir : " + file.getAbsolutePath());
        if (file.isDirectory()) {
            addURL(file);
            File[] subFiles = file.listFiles();
            if (subFiles != null) {
                for (File tmp : subFiles) {
                    loadResource(tmp);
                }
            }
        }
    }

    /**
     * load Classpath by Dir
     *
     * @param file jar插件的Dir
     * @throws IOException 加载所有的类插件jar包
     */
    public static void loadClasspath(File file) throws Exception {
//        System.out.println("* >>> load Classpath of dir : " + file.getAbsolutePath());
        if (file.isDirectory()) {
            File[] subFiles = file.listFiles();
            if (subFiles != null) {
                for (File subFile : subFiles) {
                    loadClasspath(subFile);
                }
            }
        } else {
            if (file.getAbsolutePath().endsWith(JAR_SUFFIX) || file.getAbsolutePath().endsWith(ZIP_SUFFIX)) {
                addURL(file);
            }
        }
    }
}