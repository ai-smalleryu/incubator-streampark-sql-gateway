package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.service.application.ApplicationService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationServiceImpl implements ApplicationService {

  // todo bean hold map of application info
  @Override
  public void start(Application application, boolean auto) throws Exception {
    getApplicationService(application).start(application, auto);
  }

  @Override
  public void restart(Application application) throws Exception {
    getApplicationService(application).restart(application);
  }

  @Override
  public void delete(Application application) throws Exception {
    getApplicationService(application).delete(application);
  }

  @Override
  public void cancel(Application application) throws Exception {
    getApplicationService(application).cancel(application);
  }

  /**
   * get application service by application type
   *
   * @param application
   * @return
   */
  private ApplicationService getApplicationService(Application application) {
    // todo get application service by application type
    ApplicationService applicationService = null;
    return null;
  }
}
