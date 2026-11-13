package org.prats.definitions.interfaces;

import java.util.List;

public interface ServiceRegistry {

    void registerService(String serviceId, Service service);

    void registerService(Service service);

    String getServiceId(Service service);

    <T extends Service> T getService(
            String serviceId,
            Class<T> serviceClass
    );

    <T extends Service> T getService(Class<T> serviceClass);

    List<Service> getAllServices();

    void clearRegistry();

    int getSize();
}