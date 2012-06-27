package com.hazelblast.client.basic;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static java.lang.String.format;

/**
 * A {@link MethodInvocationHandlerFactory} that is responsible for producing {@link MethodInvocationHandler} for the
 * toString/hashCode/equals methods.
 *
 * @author Peter Veentjer.
 */
public class ToStringEqualsHashCodeInvocationHandlerFactory extends MethodInvocationHandlerFactory {

    @Override
    public Class<? extends Annotation> getAnnotationClass() {
        throw new UnsupportedOperationException();
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
