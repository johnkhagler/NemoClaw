#!/usr/bin/env node
// SPDX-FileCopyrightText: Local MCP stdio-to-SSE bridge for DJ DEV
// SPDX-License-Identifier: Apache-2.0
//
// Bridges stdio (used by acpx MCP plugin spawn) to an external SSE MCP server.
// Usage: node mcp-sse-bridge.js <SSE_URL>
// e.g.:  node mcp-sse-bridge.js http://172.17.0.1:5010/sse
//
// Protocol:
//   1. Connect to SSE_URL, receive "event: endpoint\ndata: <POST_URL>"
//   2. For each JSON-RPC line on stdin, POST to POST_URL, write response to stdout
//   3. Keep SSE connection alive (keepalive comments ignored)

"use strict";

const http = require("http");
const https = require("https");
const readline = require("readline");

const SSE_URL = process.argv[2];
if (!SSE_URL) {
  process.stderr.write("Usage: node mcp-sse-bridge.js <SSE_URL>\n");
  process.exit(1);
}

const parsed = new URL(SSE_URL);
const transport = parsed.protocol === "https:" ? https : http;

let messageUrl = null;
let messageQueue = [];
let ready = false;

function log(msg) {
  process.stderr.write(`[mcp-bridge] ${msg}\n`);
}

function postMessage(body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const msgParsed = new URL(messageUrl);
    const options = {
      hostname: msgParsed.hostname,
      port: msgParsed.port || (msgParsed.protocol === "https:" ? 443 : 80),
      path: msgParsed.pathname,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data),
      },
    };
    const req = (msgParsed.protocol === "https:" ? https : http).request(options, (res) => {
      let buf = "";
      res.on("data", (c) => (buf += c));
      res.on("end", () => {
        if (res.statusCode === 202 || res.statusCode === 200) {
          resolve(buf);
        } else if (buf) {
          try {
            resolve(JSON.parse(buf));
          } catch {
            resolve(buf);
          }
        } else {
          resolve(null);
        }
      });
    });
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

// Connect to SSE endpoint and read the message URL
function connectSse() {
  const options = {
    hostname: parsed.hostname,
    port: parsed.port || (parsed.protocol === "https:" ? 443 : 80),
    path: parsed.pathname,
    method: "GET",
    headers: { Accept: "text/event-stream" },
  };

  const req = transport.request(options, (res) => {
    if (res.statusCode !== 200) {
      log(`SSE connection failed: HTTP ${res.statusCode}`);
      process.exit(1);
    }
    log(`SSE connected (${res.statusCode})`);

    let buf = "";
    let eventType = "";

    res.on("data", (chunk) => {
      buf += chunk.toString();
      const lines = buf.split("\n");
      buf = lines.pop(); // keep incomplete line

      for (const line of lines) {
        if (line.startsWith("event:")) {
          eventType = line.slice(6).trim();
        } else if (line.startsWith("data:")) {
          const data = line.slice(5).trim();
          if (eventType === "endpoint") {
            messageUrl = data;
            log(`Message endpoint: ${messageUrl}`);
            ready = true;
            // Flush queued messages
            for (const msg of messageQueue) {
              sendMessage(msg);
            }
            messageQueue = [];
          } else if (eventType === "message") {
            // Server-sent message — write to stdout
            try {
              const obj = JSON.parse(data);
              process.stdout.write(JSON.stringify(obj) + "\n");
            } catch {
              // ignore non-JSON SSE data
            }
          }
          eventType = "";
        }
        // ignore keepalive comments (": keepalive")
      }
    });

    res.on("end", () => {
      log("SSE connection closed, reconnecting in 2s...");
      ready = false;
      setTimeout(connectSse, 2000);
    });
  });

  req.on("error", (err) => {
    log(`SSE request error: ${err.message}, retrying in 2s...`);
    setTimeout(connectSse, 2000);
  });

  req.end();
}

async function sendMessage(msg) {
  if (!ready || !messageUrl) {
    messageQueue.push(msg);
    return;
  }
  try {
    const result = await postMessage(msg);
    if (result && typeof result === "object") {
      process.stdout.write(JSON.stringify(result) + "\n");
    }
  } catch (err) {
    log(`POST error: ${err.message}`);
    // Write a JSON-RPC error reply
    const errReply = {
      jsonrpc: "2.0",
      id: msg.id ?? null,
      error: { code: -32603, message: `Bridge transport error: ${err.message}` },
    };
    process.stdout.write(JSON.stringify(errReply) + "\n");
  }
}

// Read JSON-RPC from stdin line by line
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  if (!line.trim()) return;
  try {
    const msg = JSON.parse(line);
    sendMessage(msg);
  } catch {
    log(`Invalid JSON on stdin: ${line}`);
  }
});
rl.on("close", () => {
  log("stdin closed, exiting");
  process.exit(0);
});

connectSse();
