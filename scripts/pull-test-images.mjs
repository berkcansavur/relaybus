// Pulls every Docker image the integration tests use, in parallel, so the first
// `pnpm test` does not spend its time (or hit a start timeout) on downloads.
// Keep this list in sync with the images in packages/*/test (postgres-server.ts,
// mongo-server.ts, broker-server.ts, kafka-server.ts).
import { spawn } from 'node:child_process';

const images = [
  'postgres:17-alpine',
  'postgres:13-alpine',
  'mongo:7',
  'rabbitmq:4-alpine',
  'confluentinc/cp-kafka:7.5.0',
];

function pull(image) {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', ['pull', '--quiet', image], {
      stdio: ['ignore', 'ignore', 'inherit'],
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        console.log(`pulled ${image}`);
        resolve();
      } else {
        reject(new Error(`docker pull ${image} exited with ${code}`));
      }
    });
  });
}

await Promise.all(images.map(pull));
