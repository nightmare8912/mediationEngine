package org.prats.registries;

import org.prats.services.Service;

import java.util.List;

public interface ServiceRegistry {
    void registerService(Service service);
    <T extends Service> Service getService(Class<T> serviceClass);
    List<Service> getAllServices();
    void clearRegistry();
    int getSize();
}