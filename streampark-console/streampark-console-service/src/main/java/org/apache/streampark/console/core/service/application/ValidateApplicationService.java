package org.apache.streampark.console.core.service.application;

import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.core.entity.Application;

public interface ValidateApplicationService {

    //region check application
    boolean checkEnv(Application app) throws ApplicationException;

    boolean checkAlter(Application application);

    boolean checkBuildAndUpdate(Application app);

    boolean existsRunningJobByClusterId(Long clusterId);

    boolean existsByTeamId(Long teamId);

    boolean existsJobByClusterId(Long id);
    //endregion

}
