const activeSubscriptions = new Map();

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get('Accept') === 'application/nostr+json') {
      return new Response(JSON.stringify({
        name: "Cloudflare Workers Nostr Relay",
        description: "Nostr relay running on Cloudflare Workers",
        pubkey: "your_pubkey_hex",
        supported_nips: [1, 11, 20],
        software: "https://github.com"
      }), {
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Connect via WebSocket or check NIP-11.', { status: 400 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    server.addEventListener('message', async (event) => {
      try {
        const message = JSON.parse(event.data);
        if (!Array.isArray(message)) return;
        const type = message[0];

        if (type === 'EVENT' && message[1]) {
          const ev = message[1];
          await env.DB.prepare(
            "INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig) VALUES (?, ?, ?, ?, ?, ?, ?)"
          ).bind(ev.id, ev.pubkey, ev.created_at, ev.kind, JSON.stringify(ev.tags), ev.content, ev.sig).run();

          server.send(JSON.stringify(["OK", ev.id, true, ""]));

          for (const [subId, sub] of activeSubscriptions.entries()) {
            if (!sub.filters.kinds || sub.filters.kinds.includes(ev.kind)) {
              sub.ws.send(JSON.stringify(["EVENT", subId, ev]));
            }
          }
        }

        if (type === 'REQ' && typeof message[1] === 'string' && message[2]) {
          const subId = message[1];
          const filters = message[2];
          activeSubscriptions.set(subId, { ws: server, filters });

          let query = "SELECT * FROM events WHERE 1=1";
          const params = [];
          if (filters.kinds && filters.kinds.length > 0) {
            query += ` AND kind IN (${filters.kinds.map(() => '?').join(',')})`;
            params.push(...filters.kinds);
          }
          if (filters.authors && filters.authors.length > 0) {
            query += ` AND pubkey IN (${filters.authors.map(() => '?').join(',')})`;
            params.push(...filters.authors);
          }
          query += " ORDER BY created_at DESC LIMIT ?";
          params.push(filters.limit || 50);

          const { results } = await env.DB.prepare(query).bind(...params).all();
          for (const row of results) {
            server.send(JSON.stringify(["EVENT", subId, {
              id: row.id,
              pubkey: row.pubkey,
              created_at: row.created_at,
              kind: row.kind,
              tags: JSON.parse(row.tags),
              content: row.content,
              sig: row.sig
            }]));
          }
          server.send(JSON.stringify(["EOSE", subId]));
        }

        if (type === 'CLOSE' && typeof message[1] === 'string') {
          activeSubscriptions.delete(message[1]);
        }
      } catch (err) {
        server.send(JSON.stringify(["NOTICE", "Error processing request"]));
      }
    });

    server.addEventListener('close', () => {
      for (const [subId, sub] of activeSubscriptions.entries()) {
        if (sub.ws === server) activeSubscriptions.delete(subId);
      }
    });

    return new Response(null, { status: 101, webSocket: client });
  }
};
