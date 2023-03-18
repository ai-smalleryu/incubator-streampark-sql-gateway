package org.apache.streampark.console.core.service.application;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.JobID;
import org.apache.streampark.common.conf.ConfigConst;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.enums.ConfigFileType;
import org.apache.streampark.console.core.mapper.ApplicationConfigMapper;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkEnvService;

import java.util.Date;

@RequiredArgsConstructor
public abstract class AbstractApplicationService extends ServiceImpl<ApplicationMapper, Application> implements BaseApplicationService {

    private final FlinkEnvService flinkEnvService;
    private final ApplicationConfigMapper configService;

    private final ApplicationService applicationService;


    protected void beforeApplicationStartCheck(Application appParam, boolean auto) {
        final Application application = getById(appParam.getId());

        Utils.notNull(application);
        application.setAllowNonRestored(appParam.getAllowNonRestored());

        FlinkEnv flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
        if (flinkEnv == null) {
            throw new ApiAlertException("[StreamPark] can no found flink version");
        }

        ExecutionMode executionMode = ExecutionMode.of(application.getExecutionMode());
        ApiAlertException.throwIfNull(
            executionMode, "ExecutionMode can't be null, start application failed.");

        // if manually started, clear the restart flag
        if (!auto) {
            application.setRestartCount(0);
        } else {
            if (!application.isNeedRestartOnFailed()) {
                return;
            }
            appParam.setSavePointed(true);
            application.setRestartCount(application.getRestartCount() + 1);
        }
        // set the latest to Effective, (it will only become the current effective at this time)
        applicationService.toEffective(application);

        ApplicationConfig applicationConfig = configService.getEffective(application.getId());
        if (application.isCustomCodeJob()) {
            if (application.isUploadJob()) {
                appConf =
                    String.format(
                        "json://{\"%s\":\"%s\"}",
                        ConfigConst.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
            } else {
                switch (application.getApplicationType()) {
                    case STREAMPARK_FLINK:
                        ConfigFileType fileType = ConfigFileType.of(applicationConfig.getFormat());
                        if (fileType != null && !fileType.equals(ConfigFileType.UNKNOWN)) {
                            appConf =
                                String.format("%s://%s", fileType.getTypeName(), applicationConfig.getContent());
                        } else {
                            throw new IllegalArgumentException(
                                "application' config type error,must be ( yaml| properties| hocon )");
                        }
                        break;
                    case APACHE_FLINK:
                        appConf =
                            String.format(
                                "json://{\"%s\":\"%s\"}",
                                ConfigConst.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
                        break;
                    default:
                        throw new IllegalArgumentException(
                            "[StreamPark] ApplicationType must be (StreamPark flink | Apache flink)... ");
                }
            }

        }
    }
}
