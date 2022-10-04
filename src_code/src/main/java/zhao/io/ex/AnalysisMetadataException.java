package zhao.io.ex;

/**
 * 解析元数据错误异常
 */
public class AnalysisMetadataException extends DataTearException {
    public AnalysisMetadataException() {
        super();
    }

    public AnalysisMetadataException(String message) {
        super(message);
    }

    public AnalysisMetadataException(String errorStr, org.slf4j.Logger logger) {
        super(errorStr);
        super.LoggerToFile(this, logger);
    }

    @Override
    public String getMessage() {
        return super.getMessage() + "NameManager的元数据解析失败！";
    }

    @Override
    public String getLocalizedMessage() {
        return super.getLocalizedMessage() + "NameManager的元数据解析失败！";
    }
}
