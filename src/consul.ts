import { hostname, networkInterfaces } from "node:os";

export interface ConsulRegistration {
  // Compose service name to register as, e.g. "listing". This is what
  // discovery resolves against, so it must match the compose service name
  // (NOT OTEL_SERVICE_NAME).
  name: string;
  // The gRPC port the service is bound to.
  port: number;
  // Address other services reach this instance at. Defaults to this
  // container's routable IPv4. APISIX's service-discovery balancer can only
  // dial IPs (it does NOT DNS-resolve discovery nodes), so we must register an
  // IP, not the compose hostname.
  address?: string;
  // Unique id per instance. Defaults to `${name}-${hostname()}`.
  id?: string;
  // Consul agent URL. Defaults to CONSUL_HTTP_ADDR or http://consul:8500.
  consulUrl?: string;
}

function resolveConsulUrl(consulUrl?: string): string {
  return consulUrl ?? process.env.CONSUL_HTTP_ADDR ?? "http://consul:8500";
}

// First non-internal IPv4 of this container. APISIX discovery dials node hosts
// directly without DNS resolution, so Consul must hold an IP rather than the
// compose service name.
function localIPv4(): string | undefined {
  for (const iface of Object.values(networkInterfaces())) {
    for (const ni of iface ?? []) {
      if (ni.family === "IPv4" && !ni.internal) return ni.address;
    }
  }
  return undefined;
}

/**
 * Registers this service instance with Consul, including a TCP health check
 * that Consul polls so dead instances are pruned automatically.
 *
 * Returns an idempotent deregister function to call on graceful shutdown.
 * Registration failures are logged but never thrown — a Consul hiccup should
 * not take down an otherwise healthy gRPC server.
 */
export async function registerWithConsul(
  reg: ConsulRegistration
): Promise<() => Promise<void>> {
  const consulUrl = resolveConsulUrl(reg.consulUrl);
  const address = reg.address ?? localIPv4() ?? reg.name;
  const id = reg.id ?? `${reg.name}-${hostname()}`;

  const body = {
    Name: reg.name,
    ID: id,
    Address: address,
    Port: reg.port,
    Check: {
      TCP: `${address}:${reg.port}`,
      Interval: "10s",
      DeregisterCriticalServiceAfter: "1m"
    }
  };

  try {
    const response = await fetch(`${consulUrl}/v1/agent/service/register`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      console.error(
        `Consul registration for "${id}" failed: ${response.status} ${response.statusText}`
      );
    } else {
      console.log(`Registered "${id}" with Consul at ${consulUrl}`);
    }
  } catch (error) {
    console.error(`Consul registration for "${id}" errored:`, error);
  }

  let deregistered = false;
  return async function deregister(): Promise<void> {
    if (deregistered) return;
    deregistered = true;

    try {
      const response = await fetch(
        `${consulUrl}/v1/agent/service/deregister/${id}`,
        { method: "PUT" }
      );

      if (!response.ok) {
        console.error(
          `Consul deregistration for "${id}" failed: ${response.status} ${response.statusText}`
        );
      } else {
        console.log(`Deregistered "${id}" from Consul`);
      }
    } catch (error) {
      console.error(`Consul deregistration for "${id}" errored:`, error);
    }
  };
}
