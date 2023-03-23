package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.service.application.ValidateApplicationService;
import org.springframework.stereotype.Service;

@Service
public class ValidateApplicationServiceImpl implements ValidateApplicationService {
    @Override
    public boolean checkEnv(Application app) throws ApplicationException {
        return false;
    }

    @Override
    public boolean checkAlter(Application application) {
        return false;
    }

    @Override
    public boolean checkBuildAndUpdate(Application app) {
        return false;
    }

    @Override
    public boolean existsRunningJobByClusterId(Long clusterId) {
        return false;
    }

    @Override
    public boolean existsByTeamId(Long teamId) {
        return false;
    }

    @Override
    public boolean existsJobByClusterId(Long id) {
        return false;
    }
}
