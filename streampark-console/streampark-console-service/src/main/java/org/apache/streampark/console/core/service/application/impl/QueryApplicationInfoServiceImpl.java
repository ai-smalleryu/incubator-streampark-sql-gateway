package org.apache.streampark.console.core.service.application.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.enums.AppExistsState;
import org.apache.streampark.console.core.service.application.QueryApplicationInfoService;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Service
public class QueryApplicationInfoServiceImpl implements QueryApplicationInfoService {
    @Override
    public AppExistsState checkExists(Application app) {
        return null;
    }

    @Override
    public String checkSavepointPath(Application app) throws Exception {
        return null;
    }

    @Override
    public String readConf(Application app) throws IOException {
        return null;
    }

    @Override
    public Application getApp(Application app) {
        return null;
    }

    @Override
    public String getMain(Application application) {
        return null;
    }

    @Override
    public IPage<Application> page(Application app, RestRequest request) {
        return null;
    }

    @Override
    public String getYarnName(Application app) {
        return null;
    }

    @Override
    public Map<String, Serializable> dashboard(Long teamId) {
        return null;
    }

    @Override
    public List<Application> getByProjectId(Long id) {
        return null;
    }

    @Override
    public List<Application> getByTeamId(Long teamId) {
        return null;
    }

    @Override
    public List<Application> getByTeamIdAndExecutionModes(Long teamId, Collection<ExecutionMode> executionModes) {
        return null;
    }
}
