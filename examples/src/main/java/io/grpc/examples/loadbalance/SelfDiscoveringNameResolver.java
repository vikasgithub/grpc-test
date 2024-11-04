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
import io.grpc.*;
import io.grpc.examples.helloworld.HelloWorldClient;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static io.grpc.examples.loadbalance.LoadBalanceClient.*;

public class SelfDiscoveringNameResolver extends NameResolver {

    static private final int[] SERVER_PORTS = {50051, 50052, 50053};
    private Listener2 listener;

    private final URI uri;

    private Map<String,Set<InetSocketAddress>> addrStore = new HashMap<>();

    public SelfDiscoveringNameResolver(URI targetUri) {
        this.uri = targetUri;
        // This is a fake name resolver, so we just hard code the address here
        updateAddrStore();
    }

    private void updateAddrStore() {
        String[] servers = getServers();
        for (String server : servers) {
            try {
                ManagedChannel channel = ManagedChannelBuilder.forTarget(server)
                        .enableRetry()
                        .maxRetryAttempts(3)
                        .usePlaintext()
                        .build();
                HelloWorldClient client = new HelloWorldClient(channel);
                List<String> models = client.getModels();
                for (String model : models) {
                    String temp = model + ".grpc.io";
                    Set<InetSocketAddress> addresses = this.addrStore.get(temp);
                    if (addresses == null) {
                        addresses = new HashSet<>();
                    }
                    String[] split = server.split(":");
                    addresses.add(new InetSocketAddress(split[0], Integer.parseInt(split[1])));
                    this.addrStore.put(temp, addresses);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        System.out.println(this.addrStore);
    }

    private static String[] getServers() {
        Properties prop = new Properties();
        InputStream stream = null;
        try {
            stream = Files.newInputStream(Paths.get("/Users/vikas/trainings/grpc-java/grpc-java-master/examples/src/main/java/io/grpc/examples/loadbalance/server2.properties"));
            prop.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("servers: " + prop.get("servers").toString());
        return prop.get("servers").toString().split(",");
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

        ReResolveTask task = new ReResolveTask(this);
        task.start();
    }

    @Override
    public void refresh() {
        this.resolve();
    }

    private void resolve() {
        System.out.println("Inside Resolve");
        updateAddrStore();
        Set<InetSocketAddress> addresses = addrStore.get(uri.getPath().substring(1));
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
