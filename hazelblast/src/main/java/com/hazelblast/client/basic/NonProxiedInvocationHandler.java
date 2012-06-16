package com.hazelblast.client.basic;

import com.hazelblast.api.exceptions.RemotingException;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

public class NonProxiedInvocationHandler implements MethodInvocationHandler {

    private final RemoteInterfaceInfo remoteInterfaceInfo;
    private final String methodName;
    private final RemoteMethodInfo remoteMethodInfo;

    public NonProxiedInvocationHandler(RemoteInterfaceInfo remoteInterfaceInfo, RemoteMethodInfo remoteMethodInfo) {
        this.remoteInterfaceInfo = notNull("remoteInterfaceInfo", remoteInterfaceInfo);
        this.remoteMethodInfo = notNull("remoteMethodInfo", remoteMethodInfo);
        this.methodName = remoteMethodInfo.method.getName();
    }

    public boolean isLocal() {
        return true;
    }

    public Object invoke(Object proxy,  Object[] args) throws Throwable {
        if (methodName.equals("toString")) {
            return remoteInterfaceInfo.targetInterface.getName() + "@" + System.identityHashCode(proxy);
        } else if (methodName.equals("hashCode")) {
            return System.identityHashCode(proxy);
        } else if (methodName.equals("equals")) {
            return proxy == args[0];
        } else {
            throw new RemotingException(format("unhandled method '%s'", remoteMethodInfo.method));
        }
    }
}
