// Minimal static file server that sets the headers the threaded wasm build needs:
//   - COOP: same-origin
//   - COEP: credentialless   (lets cross-origin fetches to hub/CAS through without
//                              requiring them to send CORP, while still giving the
//                              page crossOriginIsolated → SharedArrayBuffer)
// Usage: node server.mjs <doc_root>     PORT env optional (default 8765)

import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve(process.argv[2] || '.');
const port = parseInt(process.env.PORT || '8765', 10);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.ts': 'application/typescript; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
};

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    let p = path.normalize(path.join(root, decodeURIComponent(url.pathname)));
    if (!p.startsWith(root)) {
      res.writeHead(403).end('forbidden');
      return;
    }
    const stat = await fs.stat(p).catch(() => null);
    if (!stat) {
      res.writeHead(404).end(`not found: ${url.pathname}`);
      return;
    }
    if (stat.isDirectory()) p = path.join(p, 'index.html');
    const data = await fs.readFile(p);
    const ext = path.extname(p).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'credentialless',
      'Cross-Origin-Resource-Policy': 'same-origin',
      'Cache-Control': 'no-store',
    });
    res.end(data);
  } catch (e) {
    res.writeHead(500).end(String(e?.message || e));
  }
});

server.listen(port, '127.0.0.1', () => {
  console.log(`serving ${root} on http://127.0.0.1:${port}`);
});
