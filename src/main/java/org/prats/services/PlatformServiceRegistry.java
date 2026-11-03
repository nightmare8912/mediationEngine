package org.prats.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlatformServiceRegistry implements ServiceRegistry {

    private final Map<String, Service> services;

    public PlatformServiceRegistry() {
        this.services = new HashMap<>();
    }

    @Override
    public void registerService(
            Service service
    ) {
        services.put(
                service.getServiceName(),
                service
        );
    }

    @Override
    public <T extends Service> Service getService(Class<T> serviceClass) {
        return serviceClass.cast(
                services.get(serviceClass.getSimpleName())
        );
    }

    @Override
    public List<Service> getAllServices() {
        return List.copyOf(services.values());
    }
}
