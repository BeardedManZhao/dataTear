package zhao.io.ex;

/**
 * API调用异常
 */
public class ZHAOLackOfInformation extends DataTearException {

    public ZHAOLackOfInformation(String s) {
        super(s);
    }

    public ZHAOLackOfInformation(String errorStr, org.slf4j.Logger logger) {
        super(errorStr);
        super.LoggerToFile(this, logger);
    }

    @Override
    public String getMessage() {
        return super.getMessage() + "您的API调用设置不全哦！！";
    }
}
