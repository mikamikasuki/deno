const encoder = new TextEncoder();

for (const keepResponse of [false, true]) {
  const cancelled = Promise.withResolvers();
  let serverBody;
  const server = Deno.serve(
    { port: 0, onListen() {} },
    () =>
      new Response(
        new ReadableStream({
          start(controller) {
            serverBody = controller;
            controller.enqueue(encoder.encode("hello"));
          },
          cancel() {
            cancelled.resolve();
          },
        }),
      ),
  );
  const aborter = new AbortController();
  const { ref, reader, retained } = await (async () => {
    const response = await fetch(
      `http://${server.addr.hostname}:${server.addr.port}`,
      { signal: aborter.signal },
    );
    return {
      ref: new WeakRef(response),
      reader: response.body.getReader(),
      retained: keepResponse ? response : undefined,
    };
  })();
  await reader.read();
  const pending = reader.read();
  void pending.catch(() => undefined);
  await new Promise((resolve) => setTimeout(resolve, 0));
  Reflect.get(globalThis, "gc")();
  await new Promise((resolve) => setTimeout(resolve, 0));
  Reflect.get(globalThis, "gc")();
  const responseAlive = !!ref.deref();
  aborter.abort();

  let timer;
  let wasCancelled = true;
  try {
    await Promise.race([
      cancelled.promise,
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("server never cancelled")),
          1000,
        );
      }),
    ]);
  } catch {
    wasCancelled = false;
    serverBody.close();
  } finally {
    clearTimeout(timer);
  }

  console.log({ keepResponse, responseAlive, cancelled: wasCancelled });
  if (keepResponse) console.log(retained.status);
  await pending.catch(() => undefined);
  reader.releaseLock();
  await server.shutdown();
}
