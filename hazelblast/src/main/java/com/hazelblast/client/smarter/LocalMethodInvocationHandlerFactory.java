package com.hazelblast.client.smarter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static java.lang.String.format;

public class LocalMethodInvocationHandlerFactory extends MethodInvocationHandlerFactory {

    @Override
    public Class<? extends Annotation> getAnnotationClass() {
        return null;
    }

    public MethodInvocationHandler build(Method method) {
        return new LocalMethodInvocationHandler(method);
    }

    class LocalMethodInvocationHandler implements MethodInvocationHandler {
        private final Method method;

        public LocalMethodInvocationHandler(Method method) {
            this.method = method;
        }

        public Object invoke(Object proxy, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (methodName.equals("toString")) {
                return method.getDeclaringClass().getName() + "@" + System.identityHashCode(proxy);
            } else if (methodName.equals("hashCode")) {
                return System.identityHashCode(proxy);
            } else if (methodName.equals("equals")) {
                return proxy == args[0];
            } else {
                throw new RuntimeException(format("unhandled method '%s'", method));
            }
        }
    }
}
