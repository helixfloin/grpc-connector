package com.acme.connectors.grpc;

import helixflo.example.grpc.GreeterGrpc;
import helixflo.example.grpc.HelloReply;
import helixflo.example.grpc.HelloRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.helixflo.core.Mediator;
import io.helixflo.core.MessageContext;
import io.helixflo.core.exception.MediationException;
import io.helixflo.core.util.Expr;
import io.helixflo.core.util.Params;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Example third-party mediator, built and versioned independently of the HelixFlo core,
 * packaged as its own shaded jar (see this module's pom.xml) and dropped into
 * deployment/connectors/. Calls a gRPC service's "Greeter.SayHello" RPC as a "class" step -
 * the connector counterpart to the framework's own built-in "http-call" step, for talking to a
 * gRPC (rather than REST) backend. Wired into a flow via:
 *
 * <pre>
 * - class:
 *     name: com.acme.connectors.grpc.GrpcGreeterMediator
 *     input:
 *       name: "${callerName}"     # supports ${...} interpolation, same as every built-in connector
 *     resultProperty: greeting    # optional - defaults to "greeting"
 * </pre>
 *
 * The target host/port come from the deploying project's globals.yaml constants - GRPC_HOST/
 * GRPC_PORT - falling back to localhost:50051 (the conventional default port used by grpc-java's
 * own "helloworld" example server) if either is omitted. Not read in the constructor - there's no
 * MessageContext yet at that point (a "class" step is instantiated once at deploy time, before any
 * request exists) - so the channel/stub are instead built lazily on the first mediate() call and
 * cached from then on, the same lazy-client pattern the other example connectors here use (see
 * e.g. s3-upload-connector).
 */
public class GrpcGreeterMediator implements Mediator {

    private final Map<String, Object> input;
    private final String resultProperty;

    private volatile ManagedChannel channel;
    private volatile GreeterGrpc.GreeterBlockingStub stub;

    public GrpcGreeterMediator(Map<String, Object> params) {
        this.input = Params.optionalMap(params, "input");
        this.resultProperty = Params.optionalString(params, "resultProperty", "greeting");
    }

    /** Built lazily (double-checked locking - this Mediator instance is shared/reused across concurrent requests). */
    private GreeterGrpc.GreeterBlockingStub stub(MessageContext ctx) {
        GreeterGrpc.GreeterBlockingStub existing = stub;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (stub == null) {
                String host = globalOrDefault(ctx, "GRPC_HOST", "localhost");
                int port = Integer.parseInt(globalOrDefault(ctx, "GRPC_PORT", "50051"));
                // usePlaintext(): no TLS - fine for a local example server; a real deployment
                // talking to a TLS-terminated gRPC endpoint would use the default (TLS) channel
                // credentials instead.
                channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
                stub = GreeterGrpc.newBlockingStub(channel);
            }
            return stub;
        }
    }

    private static String globalOrDefault(MessageContext ctx, String name, String defaultValue) {
        Object value = ctx.getGlobal(name);
        return value == null || value.toString().isBlank() ? defaultValue : value.toString();
    }

    @Override
    public void mediate(MessageContext ctx) {
        String name = requiredInput(ctx, "name");
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        try {
            // A blocking gRPC call with no deadline can hang indefinitely if the target is
            // unreachable - 5s mirrors this repo's other external-call examples (e.g. HikariCP's
            // own connection timeout).
            HelloReply reply = stub(ctx).withDeadlineAfter(5, TimeUnit.SECONDS).sayHello(request);
            ctx.setProperty(resultProperty, reply.getMessage());
        } catch (io.grpc.StatusRuntimeException e) {
            throw new MediationException("gRPC call to Greeter/SayHello failed: " + e.getStatus(), e);
        }
    }

    /** Every "input" value supports ${...} interpolation, resolved fresh per request - same convention every built-in connector's own params follow. */
    private String requiredInput(MessageContext ctx, String key) {
        Object raw = input.get(key);
        if (raw == null) {
            throw new IllegalArgumentException("grpc-greeter: missing required 'input." + key + "'");
        }
        return Expr.resolve(raw.toString(), ctx);
    }

    /** Releases the channel - see Mediator#onUndeploy(): called once when this project is undeployed/redeployed, so a stale gRPC connection doesn't leak across redeploys. */
    @Override
    public void onUndeploy() {
        ManagedChannel c = channel;
        if (c != null) {
            c.shutdownNow();
            try {
                c.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
