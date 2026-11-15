package org.prats.registries;

import org.prats.definitions.interfaces.Service;
import org.prats.definitions.interfaces.ServiceRegistry;
import org.prats.loggers.EngineLogger;

import java.util.*;

public class ContainerServiceRegistry implements ServiceRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(ContainerServiceRegistry.class);

    private final Map<String, Service> services;

    public ContainerServiceRegistry() {
        this.services = new LinkedHashMap<>();

        log.debug(
                "[ContainerServiceRegistry][constructor] Service registry initialized"
        );
    }

    @Override
    public void registerService(String serviceId, Service service) {

        Objects.requireNonNull(serviceId, "Service ID cannot be null");
        Objects.requireNonNull(service, "Service cannot be null");

        if (serviceId.isBlank()) {
            log.warn(
                    "[ContainerServiceRegistry][registerService] Registration rejected because service ID is blank"
            );

            throw new IllegalArgumentException(
                    "Service ID cannot be blank"
            );
        }

        if (services.containsKey(serviceId)) {
            log.warn(() ->
                    "[ContainerServiceRegistry][registerService] Registration rejected because service ID already exists"
                            + " | serviceId=" + serviceId
                            + " | registeredServices=" + services.size()
            );

            throw new IllegalStateException(
                    "Service already registered: " + serviceId
            );
        }

        services.put(serviceId, service);

        log.info(() ->
                "[ContainerServiceRegistry][registerService] Service registered"
                        + " | serviceId=" + serviceId
                        + " | serviceType=" + service.getClass().getSimpleName()
                        + " | registeredServices=" + services.size()
        );
    }

    @Override
    public void registerService(Service service) {

        Objects.requireNonNull(service, "Service cannot be null");

        log.debug(() ->
                "[ContainerServiceRegistry][registerService] Registering service using its configured name"
                        + " | serviceType=" + service.getClass().getSimpleName()
        );

        registerService(service.getName(), service);
    }

    @Override
    public String getServiceId(Service targetService) {

        Objects.requireNonNull(
                targetService,
                "Service cannot be null"
        );

        for (Map.Entry<String, Service> entry : services.entrySet()) {

            if (entry.getValue() == targetService) {

                log.debug(() ->
                        "[ContainerServiceRegistry][getServiceId] Service ID resolved using object identity"
                                + " | serviceId=" + entry.getKey()
                                + " | serviceType=" + targetService.getClass().getSimpleName()
                );

                return entry.getKey();
            }
        }

        log.warn(() ->
                "[ContainerServiceRegistry][getServiceId] Service is not registered"
                        + " | serviceType=" + targetService.getClass().getSimpleName()
                        + " | registeredServices=" + services.size()
        );

        throw new IllegalArgumentException(
                "Service is not registered: "
                        + targetService.getClass().getSimpleName()
        );
    }

    @Override
    public <T extends Service> T getService(
            String serviceId,
            Class<T> serviceClass
    ) {

        Objects.requireNonNull(serviceId);
        Objects.requireNonNull(serviceClass);

        Service service = services.get(serviceId);

        if (service == null) {

            log.debug(() ->
                    "[ContainerServiceRegistry][getService] No service found for requested ID"
                            + " | serviceId=" + serviceId
                            + " | requestedType=" + serviceClass.getSimpleName()
            );

            return null;
        }

        if (!serviceClass.isInstance(service)) {

            log.warn(() ->
                    "[ContainerServiceRegistry][getService] Registered service does not match requested type"
                            + " | serviceId=" + serviceId
                            + " | actualType=" + service.getClass().getSimpleName()
                            + " | requestedType=" + serviceClass.getSimpleName()
            );

            throw new IllegalStateException(
                    "Service registered as '" + serviceId +
                            "' is not of type " + serviceClass.getSimpleName()
            );
        }

        T matchingService = serviceClass.cast(service);

        log.debug(() ->
                "[ContainerServiceRegistry][getService] Service resolved by ID"
                        + " | serviceId=" + serviceId
                        + " | serviceType=" + serviceClass.getSimpleName()
        );

        return matchingService;
    }

    @Override
    public <T extends Service> T getService(Class<T> serviceClass) {

        Objects.requireNonNull(serviceClass);

        T matchingService = null;

        for (Service service : services.values()) {

            if (serviceClass.isInstance(service)) {

                if (matchingService != null) {

                    log.warn(() ->
                            "[ContainerServiceRegistry][getService] Ambiguous service lookup; multiple services match requested type"
                                    + " | requestedType=" + serviceClass.getSimpleName()
                                    + " | registeredServices=" + services.size()
                    );

                    throw new IllegalStateException(
                            "Multiple services found for type: " +
                                    serviceClass.getSimpleName() +
                                    ". Use named service lookup instead."
                    );
                }

                matchingService = serviceClass.cast(service);
            }
        }

        final T resolvedService = matchingService;

        log.debug(() ->
                "[ContainerServiceRegistry][getService] Service lookup by type completed"
                        + " | requestedType=" + serviceClass.getSimpleName()
                        + " | serviceFound=" + (resolvedService != null)
        );

        return matchingService;
    }

    @Override
    public List<Service> getAllServices() {

        List<Service> serviceSnapshot = new ArrayList<>(services.values());

        log.debug(() ->
                "[ContainerServiceRegistry][getAllServices] Service registry snapshot created"
                        + " | serviceCount=" + serviceSnapshot.size()
        );

        return serviceSnapshot;
    }

    @Override
    public void clearRegistry() {

        int servicesBeforeClear = services.size();

        services.clear();

        log.info(() ->
                "[ContainerServiceRegistry][clearRegistry] Service registry cleared"
                        + " | removedServices=" + servicesBeforeClear
        );
    }

    @Override
    public int getSize() {

        log.debug(() ->
                "[ContainerServiceRegistry][getSize] Service registry size requested"
                        + " | serviceCount=" + services.size()
        );

        return services.size();
    }
}