/*
 * Copyright 2022 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.loadbalance;

import com.google.common.collect.ImmutableMap;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.grpc.examples.loadbalance.LoadBalanceClient.*;

public class ExampleNameResolver extends NameResolver {

    static private final int[] SERVER_PORTS = {50051, 50052, 50053};
    private Listener2 listener;

    private final URI uri;

    private Map<String,List<InetSocketAddress>> addrStore;

    public ExampleNameResolver(URI targetUri) {
        this.uri = targetUri;
        // This is a fake name resolver, so we just hard code the address here
        addrStore = createAddrStore();
    }

    private static ImmutableMap<String, List<InetSocketAddress>> createAddrStore() {
        //int[] ports = getPorts();
        return ImmutableMap.<String, List<InetSocketAddress>>builder()
                .put(model1ServiceName,
                        Arrays.stream(new int[]{50051})
                                .mapToObj(port -> new InetSocketAddress("localhost", port))
                                .collect(Collectors.toList())
                ).put(model2ServiceName,
                        Arrays.stream(new int[]{50052,50053})
                                .mapToObj(port -> new InetSocketAddress("localhost", port))
                                .collect(Collectors.toList())
                )
                .build();
    }

    private static int[] getPorts() {
        Properties prop = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = null;
        try {
            stream = Files.newInputStream(Paths.get("/Users/vikas/trainings/grpc-java/grpc-java-master/examples/src/main/java/io/grpc/examples/loadbalance/servers.properties"));
            prop.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("ports: " + prop.get("localhost").toString());
        String[] ports = prop.get("localhost").toString().split(",");

        int[] intPorts = new int[ports.length];
        for (int i = 0; i < ports.length; i++) {
            intPorts[i] = Integer.parseInt(ports[i]);
        }
        return intPorts;
    }

    @Override
    public String getServiceAuthority() {
        // Be consistent with behavior in grpc-go, authority is saved in Host field of URI.
        if (uri.getHost() != null) {
            return uri.getHost();
        }
        return "no host";
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void start(Listener2 listener) {
        this.listener = listener;
        this.resolve();

//        ReResolveTask task = new ReResolveTask(this);
//        task.start();
    }

    @Override
    public void refresh() {
        this.resolve();
    }

    private void resolve() {
        System.out.println("Inside Resolve");
        this.addrStore = createAddrStore();
        List<InetSocketAddress> addresses = addrStore.get(uri.getPath().substring(1));
        try {
            List<EquivalentAddressGroup> equivalentAddressGroup = addresses.stream()
                    // convert to socket address
                    .map(this::toSocketAddress)
                    // every socket address is a single EquivalentAddressGroup, so they can be accessed randomly
                    .map(Arrays::asList)
                    .map(this::addrToEquivalentAddressGroup)
                    .collect(Collectors.toList());

            ResolutionResult resolutionResult = ResolutionResult.newBuilder()
                    .setAddresses(equivalentAddressGroup)
                    .build();

            this.listener.onResult(resolutionResult);

        } catch (Exception e){
            // when error occurs, notify listener
            this.listener.onError(Status.UNAVAILABLE.withDescription("Unable to resolve host ").withCause(e));
        }
    }

    private SocketAddress toSocketAddress(InetSocketAddress address) {
        return new InetSocketAddress(address.getHostName(), address.getPort());
    }

    private EquivalentAddressGroup addrToEquivalentAddressGroup(List<SocketAddress> addrList) {
        return new EquivalentAddressGroup(addrList);
    }

    public static class ReResolveTask extends Thread {

        NameResolver nameResolver;

        ReResolveTask(NameResolver nameResolver) {
            this.nameResolver = nameResolver;
        }

        @Override
        public void run() {
            System.out.println("Inside refresh thread..");
            while (true) {
                try {
                    Thread.sleep(15000);
                    this.nameResolver.refresh();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
