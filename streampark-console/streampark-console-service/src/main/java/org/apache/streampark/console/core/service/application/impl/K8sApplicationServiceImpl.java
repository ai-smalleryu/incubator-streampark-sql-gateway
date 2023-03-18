package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.mapper.ApplicationConfigMapper;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.application.AbstractApplicationService;
import org.springframework.stereotype.Service;

@Service("K8sApplicationService")
public class K8sApplicationServiceImpl extends AbstractApplicationService {


    public K8sApplicationServiceImpl(FlinkEnvService flinkEnvService, ApplicationConfigMapper configService, ApplicationService applicationService) {
        super(flinkEnvService, configService, applicationService);
    }

    @Override
    public void start(Application appParam, boolean auto) throws Exception {

    }

    @Override
    public void cancel(Application appParam) throws Exception {

    }
}
