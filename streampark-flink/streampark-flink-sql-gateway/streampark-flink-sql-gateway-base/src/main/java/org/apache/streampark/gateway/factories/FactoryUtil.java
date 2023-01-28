package org.apache.streampark.gateway.factories;

import org.apache.streampark.common.conf.ConfigOption;
import org.apache.streampark.gateway.exception.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** Factory utils for {@link Factory}. */
public class FactoryUtil {

  static String DEFAULT_IDENTIFIER = "default";
  private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);
  public static final ConfigOption<String> SQL_GATEWAY_ENDPOINT_TYPE =
      new ConfigOption<String>(
          "streampark.sql-gateway.service",
          null,
          true,
          String.class,
          "The service to execute the request.",
          null,
          null,
          null);

  public static <T extends Factory> T discoverFactory(
      ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
    final List<Factory> factories = discoverFactories(classLoader);

    final List<Factory> foundFactories =
        factories.stream()
            .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
            .collect(Collectors.toList());

    if (foundFactories.isEmpty()) {
      throw new ValidationException(
          String.format(
              "Could not find any factories that implement '%s' in the classpath.",
              factoryClass.getName()));
    }

    final List<Factory> matchingFactories =
        foundFactories.stream()
            .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
            .collect(Collectors.toList());

    if (matchingFactories.isEmpty()) {
      throw new ValidationException(
          String.format(
              "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                  + "Available factory identifiers are:\n\n"
                  + "%s",
              factoryIdentifier,
              factoryClass.getName(),
              foundFactories.stream()
                  .map(Factory::factoryIdentifier)
                  .filter(identifier -> !DEFAULT_IDENTIFIER.equals(identifier))
                  .distinct()
                  .sorted()
                  .collect(Collectors.joining("\n"))));
    }
    if (matchingFactories.size() > 1) {
      throw new ValidationException(
          String.format(
              "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                  + "Ambiguous factory classes are:\n\n"
                  + "%s",
              factoryIdentifier,
              factoryClass.getName(),
              matchingFactories.stream()
                  .map(f -> f.getClass().getName())
                  .sorted()
                  .collect(Collectors.joining("\n"))));
    }

    return (T) matchingFactories.get(0);
  }

  static List<Factory> discoverFactories(ClassLoader classLoader) {
    final List<Factory> result = new LinkedList<>();
    ServiceLoaderUtil.load(Factory.class, classLoader)
        .forEach(
            loadResult -> {
              if (loadResult.hasFailed()) {
                if (loadResult.getError() instanceof NoClassDefFoundError) {
                  LOG.debug(
                      "NoClassDefFoundError when loading a "
                          + Factory.class
                          + ". This is expected when trying to load a format dependency but no flink-connector-files is loaded.",
                      loadResult.getError());
                  // After logging, we just ignore this failure
                  return;
                }
                throw new RuntimeException(
                    "Unexpected error when trying to load service provider for factories.",
                    loadResult.getError());
              }
              result.add(loadResult.getService());
            });
    return result;
  }
}
