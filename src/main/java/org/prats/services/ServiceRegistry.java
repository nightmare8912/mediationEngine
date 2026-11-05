package org.prats.services;

import java.util.List;

public interface ServiceRegistry {
    void registerService(Service service);
    <T extends Service> Service getService(Class<T> serviceClass);
    List<Service> getAllServices();
    void clearRegistry();
}
