package com.hazelblast.client.smarter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static java.lang.String.format;

public class LocalMethodHandler extends ProxyMethodHandler {

    @Override
    public Class<? extends Annotation> getAnnotation() {
        return null;
    }

    public Object invoke(Object proxy, ProxyMethod proxyMethod, Object[] args) {
        String methodName = proxyMethod.method.getName();
        if (methodName.equals("toString")) {
            return proxyMethod.method.getDeclaringClass().getName() + "@" + System.identityHashCode(proxy);
        } else if (methodName.equals("hashCode")) {
            return System.identityHashCode(proxy);
        } else if (methodName.equals("equals")) {
            return proxy == args[0];
        } else {
            throw new RuntimeException(format("unhandled method '%s'", proxyMethod.method));
        }
    }

    public ProxyMethod analyze(Method method) {
        throw new UnsupportedOperationException();
    }
}
