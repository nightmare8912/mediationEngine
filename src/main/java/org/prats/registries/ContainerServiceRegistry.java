package org.prats.registries;

import org.prats.definitions.interfaces.Service;
import org.prats.definitions.interfaces.ServiceRegistry;

import java.util.*;

public class ContainerServiceRegistry implements ServiceRegistry {

    private final Map<String, Service> services;

    public ContainerServiceRegistry() {
        this.services = new LinkedHashMap<>();
    }

    @Override
    public void registerService(String serviceId, Service service) {

        Objects.requireNonNull(serviceId, "Service ID cannot be null");
        Objects.requireNonNull(service, "Service cannot be null");

        if (serviceId.isBlank()) {
            throw new IllegalArgumentException(
                    "Service ID cannot be blank"
            );
        }

        if (services.containsKey(serviceId)) {
            throw new IllegalStateException(
                    "Service already registered: " + serviceId
            );
        }

        services.put(serviceId, service);
    }

    @Override
    public void registerService(Service service) {

        Objects.requireNonNull(service, "Service cannot be null");

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
                return entry.getKey();
            }
        }

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
            return null;
        }

        if (!serviceClass.isInstance(service)) {
            throw new IllegalStateException(
                    "Service registered as '" + serviceId +
                            "' is not of type " + serviceClass.getSimpleName()
            );
        }

        return serviceClass.cast(service);
    }

    @Override
    public <T extends Service> T getService(Class<T> serviceClass) {

        Objects.requireNonNull(serviceClass);

        T matchingService = null;

        for (Service service : services.values()) {

            if (serviceClass.isInstance(service)) {

                if (matchingService != null) {
                    throw new IllegalStateException(
                            "Multiple services found for type: " +
                                    serviceClass.getSimpleName() +
                                    ". Use named service lookup instead."
                    );
                }

                matchingService = serviceClass.cast(service);
            }
        }

        return matchingService;
    }

    @Override
    public List<Service> getAllServices() {

        return new ArrayList<>(services.values());
    }

    @Override
    public void clearRegistry() {

        services.clear();
    }

    @Override
    public int getSize() {

        return services.size();
    }
}