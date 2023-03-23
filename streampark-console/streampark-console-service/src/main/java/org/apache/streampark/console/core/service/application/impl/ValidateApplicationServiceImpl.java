package org.apache.streampark.console.core.service.application.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.HdfsOperator;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApiDetailException;
import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.service.application.ValidateApplicationService;
import org.apache.streampark.console.core.task.FlinkRESTAPIWatcher;
import org.springframework.stereotype.Service;

import java.net.URI;

@Service
public class ValidateApplicationServiceImpl implements ValidateApplicationService {

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


    @Override
    public boolean checkEnv(Application appParam) throws ApplicationException {
        Application application = getById(appParam.getId());
        try {
            FlinkEnv flinkEnv;
            if (application.getVersionId() != null) {
                flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
            } else {
                flinkEnv = flinkEnvService.getDefault();
            }
            if (flinkEnv == null) {
                return false;
            }
            envInitializer.checkFlinkEnv(application.getStorageType(), flinkEnv);
            envInitializer.storageInitialize(application.getStorageType());

            if (ExecutionMode.YARN_SESSION.equals(application.getExecutionModeEnum())
                || ExecutionMode.REMOTE.equals(application.getExecutionModeEnum())) {
                FlinkCluster flinkCluster = flinkClusterService.getById(application.getFlinkClusterId());
                boolean conned = flinkCluster.verifyClusterConnection();
                if (!conned) {
                    throw new ApiAlertException("the target cluster is unavailable, please check!");
                }
            }
            return true;
        } catch (Exception e) {
            log.error(Utils.stringifyException(e));
            throw new ApiDetailException(e);
        }
    }

    @Override
    public boolean checkAlter(Application application) {
        Long appId = application.getId();
        FlinkAppState state = FlinkAppState.of(application.getState());
        if (!FlinkAppState.CANCELED.equals(state)) {
            return false;
        }
        long cancelUserId = FlinkRESTAPIWatcher.getCanceledJobUserId(appId);
        long appUserId = application.getUserId();
        return cancelUserId != -1 && cancelUserId != appUserId;
    }

    private void removeApp(Application application) {
        Long appId = application.getId();
        removeById(appId);
        try {
            application
                .getFsOperator()
                .delete(application.getWorkspace().APP_WORKSPACE().concat("/").concat(appId.toString()));
            // try to delete yarn-application, and leave no trouble.
            String path =
                Workspace.of(StorageType.HDFS).APP_WORKSPACE().concat("/").concat(appId.toString());
            if (HdfsOperator.exists(path)) {
                HdfsOperator.delete(path);
            }
        } catch (Exception e) {
            // skip
        }
    }
}
