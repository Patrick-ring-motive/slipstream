function slipstream(inputStream) {
  const reader = inputStream.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = '';
  let closed = false;

  function detectShape(obj) {
    if (obj && typeof obj === 'object') {
      if (obj.object === 'chat.completion.chunk') return 'chat';
      if (typeof obj.type === 'string' && obj.type.startsWith('response.')) return 'responses';
    }
    return 'sse';
  }

  function processEvent(controller, rawEvent) {
    const lines = rawEvent.split('\n').filter(l => l.length && !l.startsWith(':'));
    const dataLines = lines.filter(l => l.startsWith('data:')).map(l => l.slice(5).trim());
    if (!dataLines.length) return;

    const dataStr = dataLines.join('\n');
    if (dataStr === '[DONE]') {
      closed = true;
      controller.close();
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(dataStr);
    } catch (err) {
      controller.error(new Error(`slipstream: malformed event JSON: ${dataStr.slice(0, 100)}`));
      closed = true;
      return;
    }

    const shape = detectShape(parsed);

    if (shape === 'chat') {
      const token = parsed?.choices?.[0]?.delta?.content;
      if (token) controller.enqueue(encoder.encode(token));
      return;
    }

    if (shape === 'responses') {
      if (parsed.type === 'response.output_text.delta' && typeof parsed.delta === 'string') {
        controller.enqueue(encoder.encode(parsed.delta));
        return;
      }
      if (parsed.type === 'response.completed') {
        closed = true;
        controller.close();
        return;
      }
      if (parsed.type === 'response.failed' || parsed.type === 'response.incomplete') {
        closed = true;
        controller.error(new Error(`slipstream: upstream ${parsed.type}: ${JSON.stringify(parsed.response?.error ?? parsed)}`));
        return;
      }
      return; // other event types (created, in_progress, etc) — drop silently
    }

    // generic sse, padding-stripped, pass raw data through
    controller.enqueue(encoder.encode(dataStr));
  }

  function drainBuffer(controller) {
    let idx;
    while (!closed && (idx = buffer.indexOf('\n\n')) !== -1) {
      const rawEvent = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      processEvent(controller, rawEvent);
    }
  }

  return new ReadableStream({
    async pull(controller) {
      while (!closed) {
        const {
          done,
          value
        } = await reader.read();

        if (done) {
          buffer += decoder.decode();
          if (buffer.trim() && !closed) processEvent(controller, buffer);
          if (!closed) controller.close();
          return;
        }

        buffer += decoder.decode(value, {
          stream: true
        });
        drainBuffer(controller);
        return; // let stream consumer pull again; buffer may still hold partial event
      }
    },
    cancel(reason) {
      return reader.cancel(reason);
    }
  });
}
