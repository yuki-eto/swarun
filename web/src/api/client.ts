import { createConnectTransport } from "@connectrpc/connect-web";
import { createClient } from "@connectrpc/connect";
import { ControllerService } from "../gen/swarun_connect";

const transport = createConnectTransport({
  baseUrl: "/", // Uses Vite proxy in development, same-origin in production
});

export const client = createClient(ControllerService, transport);
