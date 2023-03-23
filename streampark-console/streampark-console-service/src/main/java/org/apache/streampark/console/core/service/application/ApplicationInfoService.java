package org.apache.streampark.console.core.service.application;

import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.core.entity.Application;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

public interface ApplicationInfoService {

    //region Application info management
    boolean create(Application app) throws IOException;

    Long copy(Application app) throws IOException;

    boolean update(Application app);

    void starting(Application app);

    void persistMetrics(Application application);

    void toEffective(Application application);

    void updateRelease(Application application);

    void forcedStop(Application app);

    void clean(Application app);

    List<String> historyUploadJars();

    Boolean delete(Application app);

    String upload(MultipartFile file) throws Exception;

    boolean mapping(Application app);

    void revoke(Application app) throws ApplicationException;

    //endregion
}
