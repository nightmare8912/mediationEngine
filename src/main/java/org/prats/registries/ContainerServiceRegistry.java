package org.prats.registries;

import org.prats.services.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ContainerServiceRegistry implements ServiceRegistry {

    private final Map<String, Service> services;

    public ContainerServiceRegistry() {
        this.services = new LinkedHashMap<>();
    }

    @Override
    public void registerService(Service service) {
        services.put(service.getName(), service);
    }

    @Override
    public <T extends Service> T getService(Class<T> serviceClass) {
        return serviceClass.cast(
                services.get(serviceClass.getSimpleName())
        );
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