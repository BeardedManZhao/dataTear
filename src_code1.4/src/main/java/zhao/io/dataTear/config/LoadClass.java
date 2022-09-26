package zhao.io.dataTear.config;

import java.io.File;
import java.util.ArrayList;

public class LoadClass {
    static final ArrayList<Init_Plug_in> objectArrayList = new ArrayList<>();
    private static String PATH;

    public LoadClass(String jarPath) {
        PATH = jarPath;
    }

    /**
     * @param args 类全路径参数 需要以逗号分割 Class full path parameters need to be separated by commas
     * @throws Exception 加载异常 Loading exception
     */
    public static void main(String[] args) throws Exception {
        testExtClasspathLoader(args);
    }

    /**
     * 将插件实例化保存到插件列表  Save plugin instantiation to plugin list
     *
     * @param args 插件接口对接类全路径  Plug-in interface docking class full path
     * @throws Exception 加载异常  Loading exception
     */
    private static void testExtClasspathLoader(String[] args) throws Exception {
        ExtClasspathLoader.loadClasspath(new File(PATH));
        for (String classPath : args) {
            objectArrayList.add((Init_Plug_in) Class.forName(classPath).newInstance());
        }
    }
}
