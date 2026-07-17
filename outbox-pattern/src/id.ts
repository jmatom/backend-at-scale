import { randomUUID } from "node:crypto";

// A public, prefixed id like `del_9f2c…` — the kind of customer-facing id you
// expose instead of a raw sequential DB id (those leak volume and invite
// enumeration).
export function deliveryId(): string {
  return `del_${randomUUID().replace(/-/g, "").slice(0, 20)}`;
}
