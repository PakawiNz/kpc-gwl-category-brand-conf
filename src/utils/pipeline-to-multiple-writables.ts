import { Readable, Writable } from "stream";

/**
 * Defines the options for the pipeToMultipleWritables function.
 */
export interface PipeToMultipleWritablesOptions {
  /**
   * If true, calls .end() on all writable streams when the readable stream finishes.
   * If false, leaves the writable streams open.
   * @default true
   */
  closeWritablesOnEnd?: boolean;
}

/**
 * Pipes a readable stream to multiple writable streams, managing backpressure and lifecycle events.
 * @param readable The source Readable stream.
 * @param writables An array of Writable streams to pipe to.
 * @param options Configuration options for the piping behavior.
 * @returns A Promise that resolves when the operation is complete, or rejects on error.
 */
export function pipeToMultipleWritables(
  readable: Readable,
  writables: Writable[],
  options?: PipeToMultipleWritablesOptions
): Promise<void> {
  const { closeWritablesOnEnd = true } = options || {};

  return new Promise((resolve, reject) => {
    if (!writables || writables.length === 0) {
      // If no writables, consume the readable stream to prevent memory leaks
      readable.on("data", () => {}); // Consume data
      readable.on("end", () => resolve());
      readable.on("error", reject);
      return;
    }

    // Centralized error handler to destroy all streams
    function onError(err: Error) {
      // Ensure cleanup happens only once by removing other listeners
      readable.removeListener("error", onError);
      readable.removeListener("end", onEnd);
      writables.forEach((w) => w.removeListener("error", onError));

      readable.destroy(err);
      writables.forEach((writable) => writable.destroy(err));
      reject(err);
    }

    function onEnd() {
      // If the option is enabled, end all writable streams.
      if (closeWritablesOnEnd) {
        writables.forEach((writable) => {
          if (!writable.destroyed) {
            writable.end();
          }
        });
      } else {
        // If we are not closing the writables, the process is considered
        // finished as soon as the readable has ended.
        resolve();
      }
    }

    readable.on("data", (chunk) => {
      let allCanWrite = true;
      for (const writable of writables) {
        // Only write to streams that are not destroyed
        if (!writable.destroyed && !writable.write(chunk)) {
          allCanWrite = false;
        }
      }
      if (!allCanWrite) {
        readable.pause();
        // Wait for all streams that returned false to drain
        const drainingPromises = writables
          .filter((w) => !w.destroyed && w.writableNeedDrain)
          .map((w) => new Promise<void>((r) => w.once("drain", r)));

        Promise.all(drainingPromises)
          .then(() => {
            if (!readable.destroyed) {
              readable.resume();
            }
          })
          .catch(onError); // Should not happen with 'drain' but good practice
      }
    });

    readable.on("end", onEnd);
    readable.on("error", onError);

    const numWritables = writables.length;
    let finishedWritables = 0;
    writables.forEach((writable) => {
      // The 'finish' event is only relevant if we are closing the streams.
      // It signals that .end() has been called and all data has been flushed.
      if (closeWritablesOnEnd) {
        writable.on("finish", () => {
          finishedWritables++;
          if (finishedWritables === numWritables) {
            resolve();
          }
        });
      }
      writable.on("error", onError);
    });
  });
}

export function closeAllWritables(writables: Writable[], error?: Error): void {
  // Guard against null or empty input to prevent unnecessary processing.
  if (!writables) {
    return;
  }

  for (const writable of writables) {
    // Ensure the stream exists and is not already destroyed before acting on it.
    if (writable && !writable.destroyed) {
      if (error) {
        // If an error is provided, forcefully destroy the stream.
        // This is a faster but potentially data-losing way to close.
        writable.destroy(error);
      } else {
        // If no error, gracefully end the stream. This ensures all buffered
        // data is written before the stream emits 'finish'.
        writable.end();
      }
    }
  }
}
