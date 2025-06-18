import { Readable, Writable } from 'stream';

/**
 * Pipes a readable stream to multiple writable streams, managing backpressure and lifecycle events.
 * @param readable The source Readable stream.
 * @param writables An array of Writable streams to pipe to.
 * @returns A Promise that resolves when all writable streams have finished, or rejects on error.
 */
export function pipeToMultipleWritables(readable: Readable, writables: Writable[]): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!writables || writables.length === 0) {
      // If no writables, consume the readable stream to prevent memory leaks
      readable.on('data', () => {}); // Consume data
      readable.on('end', () => resolve());
      readable.on('error', reject);
      return;
    }

    const numWritables = writables.length;
    let finishedWritables = 0;

    function onError(err: Error) {
      // Ensure cleanup happens only once
      readable.destroy(err);
      writables.forEach((writable) => writable.destroy(err));
      reject(err);
    }

    readable.on('data', (chunk) => {
      let allCanWrite = true;
      for (const writable of writables) {
        if (!writable.destroyed && !writable.write(chunk)) {
          allCanWrite = false;
        }
      }
      if (!allCanWrite) {
        readable.pause();
        // Wait for all streams that returned false to drain
        const drainingPromises = writables
          .filter((w) => !w.destroyed && w.writableNeedDrain)
          .map((w) => new Promise<void>((r) => w.once('drain', r)));

        Promise.all(drainingPromises)
          .then(() => {
            if (!readable.destroyed) readable.resume();
          })
          .catch(onError); // Should not happen with 'drain' but good practice
      }
    });

    readable.on('end', () => {
      writables.forEach((writable) => {
        if (!writable.destroyed) writable.end();
      });
    });

    readable.on('error', onError);

    writables.forEach((writable) => {
      writable.on('finish', () => {
        finishedWritables++;
        if (finishedWritables === numWritables) resolve();
      });
      writable.on('error', onError);
    });
  });
}
