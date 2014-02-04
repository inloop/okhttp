/*
 * Copyright (C) 2012 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.squareup.okhttp.internal.http;

import com.squareup.okhttp.Headers;
import com.squareup.okhttp.Protocol;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.internal.AbstractOutputStream;
import com.squareup.okhttp.internal.Util;
import com.squareup.okhttp.internal.bytes.Source;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.CacheRequest;
import java.net.ProtocolException;

import static com.squareup.okhttp.internal.Util.checkOffsetAndCount;
import static com.squareup.okhttp.internal.http.StatusLine.HTTP_CONTINUE;

public final class HttpTransport implements Transport {
  public static final int DEFAULT_CHUNK_LENGTH = 1024;

  private final HttpEngine httpEngine;
  private final HttpSource source;
  private final OutputStream socketOut;

  /**
   * This stream buffers the request headers and the request body when their
   * combined size is less than MAX_REQUEST_BUFFER_LENGTH. By combining them
   * we can save socket writes, which in turn saves a packet transmission.
   * This is socketOut if the request size is large or unknown.
   */
  private OutputStream requestOut;

  public HttpTransport(HttpEngine httpEngine, OutputStream outputStream, HttpSource source) {
    this.httpEngine = httpEngine;
    this.socketOut = outputStream;
    this.requestOut = outputStream;
    this.source = source;
  }

  @Override public OutputStream createRequestBody(Request request) throws IOException {
    long contentLength = OkHeaders.contentLength(request);

    if (httpEngine.bufferRequestBody) {
      if (contentLength > Integer.MAX_VALUE) {
        throw new IllegalStateException("Use setFixedLengthStreamingMode() or "
            + "setChunkedStreamingMode() for requests larger than 2 GiB.");
      }

      if (contentLength != -1) {
        // Buffer a request body of a known length.
        writeRequestHeaders(request);
        return new RetryableOutputStream((int) contentLength);
      } else {
        // Buffer a request body of an unknown length. Don't write request
        // headers until the entire body is ready; otherwise we can't set the
        // Content-Length header correctly.
        return new RetryableOutputStream();
      }
    }

    if ("chunked".equalsIgnoreCase(request.header("Transfer-Encoding"))) {
      // Stream a request body of unknown length.
      writeRequestHeaders(request);
      return new ChunkedOutputStream(requestOut, DEFAULT_CHUNK_LENGTH);
    }

    if (contentLength != -1) {
      // Stream a request body of a known length.
      writeRequestHeaders(request);
      return new FixedLengthOutputStream(requestOut, contentLength);
    }

    throw new IllegalStateException(
        "Cannot stream a request body without chunked encoding or a known content length!");
  }

  @Override public void flushRequest() throws IOException {
    requestOut.flush();
    requestOut = socketOut;
  }

  @Override public void writeRequestBody(RetryableOutputStream requestBody) throws IOException {
    requestBody.writeToSocket(requestOut);
  }

  /**
   * Prepares the HTTP headers and sends them to the server.
   *
   * <p>For streaming requests with a body, headers must be prepared
   * <strong>before</strong> the output stream has been written to. Otherwise
   * the body would need to be buffered!
   *
   * <p>For non-streaming requests with a body, headers must be prepared
   * <strong>after</strong> the output stream has been written to and closed.
   * This ensures that the {@code Content-Length} header field receives the
   * proper value.
   */
  public void writeRequestHeaders(Request request) throws IOException {
    httpEngine.writingRequestHeaders();
    String requestLine = RequestLine.get(request,
        httpEngine.getConnection().getRoute().getProxy().type(),
        httpEngine.getConnection().getHttpMinorVersion());
    writeRequest(requestOut, request.getHeaders(), requestLine);
  }

  @Override public Response.Builder readResponseHeaders() throws IOException {
    return source.readResponse();
  }

  /** Returns bytes of a request header for sending on an HTTP transport. */
  public static void writeRequest(OutputStream out, Headers headers, String requestLine)
      throws IOException {
    StringBuilder result = new StringBuilder(256);
    result.append(requestLine).append("\r\n");
    for (int i = 0; i < headers.size(); i ++) {
      result.append(headers.name(i))
          .append(": ")
          .append(headers.value(i))
          .append("\r\n");
    }
    result.append("\r\n");
    out.write(result.toString().getBytes("ISO-8859-1"));
  }

  /** Parses bytes of a response header from an HTTP transport. */
  public static Response.Builder readResponse(InputStream in) throws IOException {
    while (true) {
      String statusLineString = Util.readAsciiLine(in);
      StatusLine statusLine = new StatusLine(statusLineString);

      Response.Builder responseBuilder = new Response.Builder()
          .statusLine(statusLine)
          .header(OkHeaders.SELECTED_TRANSPORT, Protocol.HTTP_11.name.utf8())
          .header(OkHeaders.SELECTED_PROTOCOL, Protocol.HTTP_11.name.utf8());

      Headers.Builder headersBuilder = new Headers.Builder();
      OkHeaders.readHeaders(headersBuilder, in);
      responseBuilder.headers(headersBuilder.build());

      if (statusLine.code() != HTTP_CONTINUE) return responseBuilder;
    }
  }

  @Override public boolean makeReusable(
      boolean streamCanceled, OutputStream requestBodyOut, Source responseBodyIn) {
    if (streamCanceled) {
      return false;
    }

    // We cannot reuse sockets that have incomplete output.
    if (requestBodyOut != null && !((AbstractOutputStream) requestBodyOut).isClosed()) {
      return false;
    }

    // If the request specified that the connection shouldn't be reused, don't reuse it.
    if ("close".equalsIgnoreCase(httpEngine.getRequest().header("Connection"))) {
      return false;
    }

    // If the response specified that the connection shouldn't be reused, don't reuse it.
    if (httpEngine.getResponse() != null
        && "close".equalsIgnoreCase(httpEngine.getResponse().header("Connection"))) {
      return false;
    }

    if (responseBodyIn != null) {
      return source.discardStream(httpEngine, responseBodyIn);
    }

    return true;
  }

  @Override public boolean makeReusable(
      boolean streamCanceled, OutputStream requestBodyOut, InputStream responseBodyIn) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override public Source getTransferSource(CacheRequest cacheRequest) throws IOException {
    if (!httpEngine.hasResponseBody()) {
      return source.newFixedLengthSource(httpEngine, cacheRequest, 0L);
    }

    if ("chunked".equalsIgnoreCase(httpEngine.getResponse().header("Transfer-Encoding"))) {
      return source.newChunkedSource(httpEngine, cacheRequest);
    }

    long contentLength = OkHeaders.contentLength(httpEngine.getResponse());
    if (contentLength != -1) {
      return source.newFixedLengthSource(httpEngine, cacheRequest, contentLength);
    }

    // Wrap the input stream from the connection (rather than just returning
    // "socketIn" directly here), so that we can control its use after the
    // reference escapes.
    return source.newUnknownLengthSource(httpEngine, cacheRequest);
  }

  @Override public InputStream getTransferStream(CacheRequest cacheRequest) throws IOException {
    throw new UnsupportedOperationException("TODO");
  }

  /** An HTTP body with a fixed length known in advance. */
  private static final class FixedLengthOutputStream extends AbstractOutputStream {
    private final OutputStream socketOut;
    private long bytesRemaining;

    private FixedLengthOutputStream(OutputStream socketOut, long bytesRemaining) {
      this.socketOut = socketOut;
      this.bytesRemaining = bytesRemaining;
    }

    @Override public void write(byte[] buffer, int offset, int count) throws IOException {
      checkNotClosed();
      checkOffsetAndCount(buffer.length, offset, count);
      if (count > bytesRemaining) {
        throw new ProtocolException("expected " + bytesRemaining + " bytes but received " + count);
      }
      socketOut.write(buffer, offset, count);
      bytesRemaining -= count;
    }

    @Override public void flush() throws IOException {
      if (closed) {
        return; // don't throw; this stream might have been closed on the caller's behalf
      }
      socketOut.flush();
    }

    @Override public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      if (bytesRemaining > 0) {
        throw new ProtocolException("unexpected end of stream");
      }
    }
  }

  /**
   * An HTTP body with alternating chunk sizes and chunk bodies. Chunks are
   * buffered until {@code maxChunkLength} bytes are ready, at which point the
   * chunk is written and the buffer is cleared.
   */
  private static final class ChunkedOutputStream extends AbstractOutputStream {
    private static final byte[] CRLF = { '\r', '\n' };
    private static final byte[] HEX_DIGITS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };
    private static final byte[] FINAL_CHUNK = new byte[] { '0', '\r', '\n', '\r', '\n' };

    /** Scratch space for up to 8 hex digits, and then a constant CRLF. */
    private final byte[] hex = { 0, 0, 0, 0, 0, 0, 0, 0, '\r', '\n' };

    private final OutputStream socketOut;
    private final int maxChunkLength;
    private final ByteArrayOutputStream bufferedChunk;

    private ChunkedOutputStream(OutputStream socketOut, int maxChunkLength) {
      this.socketOut = socketOut;
      this.maxChunkLength = Math.max(1, dataLength(maxChunkLength));
      this.bufferedChunk = new ByteArrayOutputStream(maxChunkLength);
    }

    /**
     * Returns the amount of data that can be transmitted in a chunk whose total
     * length (data+headers) is {@code dataPlusHeaderLength}. This is presumably
     * useful to match sizes with wire-protocol packets.
     */
    private int dataLength(int dataPlusHeaderLength) {
      int headerLength = 4; // "\r\n" after the size plus another "\r\n" after the data
      for (int i = dataPlusHeaderLength - headerLength; i > 0; i >>= 4) {
        headerLength++;
      }
      return dataPlusHeaderLength - headerLength;
    }

    @Override public synchronized void write(byte[] buffer, int offset, int count)
        throws IOException {
      checkNotClosed();
      checkOffsetAndCount(buffer.length, offset, count);

      while (count > 0) {
        int numBytesWritten;

        if (bufferedChunk.size() > 0 || count < maxChunkLength) {
          // fill the buffered chunk and then maybe write that to the stream
          numBytesWritten = Math.min(count, maxChunkLength - bufferedChunk.size());
          // TODO: skip unnecessary copies from buffer->bufferedChunk?
          bufferedChunk.write(buffer, offset, numBytesWritten);
          if (bufferedChunk.size() == maxChunkLength) {
            writeBufferedChunkToSocket();
          }
        } else {
          // write a single chunk of size maxChunkLength to the stream
          numBytesWritten = maxChunkLength;
          writeHex(numBytesWritten);
          socketOut.write(buffer, offset, numBytesWritten);
          socketOut.write(CRLF);
        }

        offset += numBytesWritten;
        count -= numBytesWritten;
      }
    }

    /**
     * Equivalent to, but cheaper than writing Integer.toHexString().getBytes()
     * followed by CRLF.
     */
    private void writeHex(int i) throws IOException {
      int cursor = 8;
      do {
        hex[--cursor] = HEX_DIGITS[i & 0xf];
      } while ((i >>>= 4) != 0);
      socketOut.write(hex, cursor, hex.length - cursor);
    }

    @Override public synchronized void flush() throws IOException {
      if (closed) {
        return; // don't throw; this stream might have been closed on the caller's behalf
      }
      writeBufferedChunkToSocket();
      socketOut.flush();
    }

    @Override public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      writeBufferedChunkToSocket();
      socketOut.write(FINAL_CHUNK);
    }

    private void writeBufferedChunkToSocket() throws IOException {
      int size = bufferedChunk.size();
      if (size <= 0) {
        return;
      }

      writeHex(size);
      bufferedChunk.writeTo(socketOut);
      bufferedChunk.reset();
      socketOut.write(CRLF);
    }
  }
}
