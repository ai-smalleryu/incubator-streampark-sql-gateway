package org.apache.streampark.console.core.service.application.deploy.impl;

import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.service.application.deploy.YarnApplicationService;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class YarnApplicationServiceImpl extends ServiceImpl<ApplicationMapper, Application>
    implements YarnApplicationService {

  @Override
  public void start(Application application, boolean auto) throws Exception {}

  @Override
  public void restart(Application application) throws Exception {}

  @Override
  public void delete(Application application) throws Exception {}

  @Override
  public void cancel(Application application) throws Exception {}

  @Override
  public String getYarnName(Application app) {
    return null;
  }
}
