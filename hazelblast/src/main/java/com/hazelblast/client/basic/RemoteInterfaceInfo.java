package com.hazelblast.client.basic;

import java.lang.reflect.Method;
import java.util.Map;

public class RemoteInterfaceInfo {
    public final Class targetInterface;
    public final Map<Method, RemoteMethodInfo> methodInfoMap;

    public RemoteInterfaceInfo(Class targetInterface, Map<Method, RemoteMethodInfo> methodInfoMap) {
        this.targetInterface = targetInterface;
        this.methodInfoMap = methodInfoMap;
    }
}
