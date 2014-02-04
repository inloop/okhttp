/*
 * Copyright (C) 2014 Square, Inc.
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

import com.squareup.okhttp.Connection;
import com.squareup.okhttp.Headers;
import com.squareup.okhttp.Protocol;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.internal.Util;
import com.squareup.okhttp.internal.bytes.Deadline;
import com.squareup.okhttp.internal.bytes.OkBuffer;
import com.squareup.okhttp.internal.bytes.OkBuffers;
import com.squareup.okhttp.internal.bytes.Source;
import java.io.IOException;
import java.io.OutputStream;
import java.net.CacheRequest;
import java.net.ProtocolException;
import java.net.Socket;

import static com.squareup.okhttp.internal.http.StatusLine.HTTP_CONTINUE;

public class HttpSource {
  private final Source source;
  private final OkBuffer buffer = new OkBuffer();

  public HttpSource(Source source) {
    this.source = source;
  }

  /** Parses bytes of a response header from an HTTP transport. */
  public Response.Builder readResponse() throws IOException {
    while (true) {
      String statusLineString = readAsciiLine();
      StatusLine statusLine = new StatusLine(statusLineString);

      Response.Builder responseBuilder = new Response.Builder()
          .statusLine(statusLine)
          .header(OkHeaders.SELECTED_TRANSPORT, Protocol.HTTP_11.name.utf8())
          .header(OkHeaders.SELECTED_PROTOCOL, Protocol.HTTP_11.name.utf8());

      Headers.Builder headersBuilder = new Headers.Builder();
      readHeaders(headersBuilder);
      responseBuilder.headers(headersBuilder.build());

      if (statusLine.code() != HTTP_CONTINUE) return responseBuilder;
    }
  }

  /** Reads headers or trailers into {@code builder}. */
  private void readHeaders(Headers.Builder builder) throws IOException {
    // parse the result headers until the first blank line
    for (String line; (line = readAsciiLine()).length() != 0; ) {
      builder.addLine(line);
    }
  }

  /**
   * Returns all characters up-to but not including the next newline sequence;
   * either {@code \r\n} or {@code \n}.
   */
  private String readAsciiLine() throws IOException {
    long newlineIndex = OkBuffers.seek(buffer, (byte) '\n', source, Deadline.NONE);
    boolean cr = newlineIndex > 0 && buffer.peekByte(newlineIndex - 1) == '\r';
    String result = buffer.readUtf8((int) (cr ? newlineIndex - 1 : newlineIndex));
    buffer.skip(cr ? 2 : 1);
    return result;
  }

  public Source newFixedLengthSource(
      HttpEngine engine, CacheRequest cacheRequest, long byteCount) throws IOException {
    return new FixedLengthSource(engine, cacheRequest, byteCount);
  }

  public Source newChunkedSource(
      HttpEngine engine, CacheRequest cacheRequest) throws IOException {
    return new ChunkedSource(engine, cacheRequest);
  }

  public Source newUnknownLengthSource(
      HttpEngine engine, CacheRequest cacheRequest) throws IOException {
    return new UnknownLengthSource(engine, cacheRequest);
  }

  /**
   * An input stream for the body of an HTTP response.
   *
   * <p>Since a single socket's input stream may be used to read multiple HTTP
   * responses from the same server, subclasses shouldn't close the socket stream.
   *
   * <p>A side effect of reading an HTTP response is that the response cache
   * is populated. If the stream is closed early, that cache entry will be
   * invalidated.
   */
  private abstract class AbstractHttpSource implements Source {
    protected final HttpEngine engine;
    private final CacheRequest cacheRequest;
    protected final OutputStream cacheBody;
    protected boolean closed;

    AbstractHttpSource(HttpEngine engine, CacheRequest cacheRequest) throws IOException {
      this.engine = engine;
      OutputStream cacheBody = cacheRequest != null ? cacheRequest.getBody() : null;

      // Some apps return a null body; for compatibility we treat that like a null cache request.
      if (cacheBody == null) cacheRequest = null;

      this.cacheBody = cacheBody;
      this.cacheRequest = cacheRequest;
    }

    protected final void cacheWrite(OkBuffer buffer, long count) throws IOException {
      if (cacheBody != null) {
        // TODO
        // cacheBody.write(buffer, offset, count);
      }
    }

    /**
     * Closes the cache entry and makes the socket available for reuse. This
     * should be invoked when the end of the body has been reached.
     */
    protected final void endOfInput() throws IOException {
      if (cacheRequest != null) cacheBody.close();
      engine.release(false);
    }

    /**
     * Calls abort on the cache entry and disconnects the socket. This
     * should be invoked when the connection is closed unexpectedly to
     * invalidate the cache entry and to prevent the HTTP connection from
     * being reused. HTTP messages are sent in serial so whenever a message
     * cannot be read to completion, subsequent messages cannot be read
     * either and the connection must be discarded.
     *
     * <p>An earlier implementation skipped the remaining bytes, but this
     * requires that the entire transfer be completed. If the intention was
     * to cancel the transfer, closing the connection is the only solution.
     */
    protected final void unexpectedEndOfInput() {
      if (cacheRequest != null) cacheRequest.abort();
      engine.release(true);
    }
  }

  /**
   * Discards the response body so that the connection can be reused. This
   * needs to be done judiciously, since it delays the current request in
   * order to speed up a potential future request that may never occur.
   *
   * <p>A stream may be discarded to encourage response caching (a response
   * cannot be cached unless it is consumed completely) or to enable connection
   * reuse.
   */
  public boolean discardStream(HttpEngine httpEngine, Source source) {
    if (source instanceof UnknownLengthSource) return false;

    Connection connection = httpEngine.getConnection();
    if (connection == null) return false;
    Socket socket = connection.getSocket();
    if (socket == null) return false;
    try {
      int socketTimeout = socket.getSoTimeout();
      socket.setSoTimeout(Transport.DISCARD_STREAM_TIMEOUT_MILLIS);
      try {
        Util.skipByReading(source, Transport.DISCARD_STREAM_TIMEOUT_MILLIS);
        return true;
      } finally {
        socket.setSoTimeout(socketTimeout);
      }
    } catch (IOException e) {
      return false;
    }
  }

  private class FixedLengthSource extends AbstractHttpSource {
    private long bytesRemaining;

    FixedLengthSource(HttpEngine engine, CacheRequest cacheRequest, long byteCount)
        throws IOException {
      super(engine, cacheRequest);
      this.bytesRemaining = byteCount;
      if (byteCount == 0) endOfInput();
    }

    @Override public long read(OkBuffer sink, long byteCount, Deadline deadline)
        throws IOException {
      if (byteCount < 0) throw new IllegalArgumentException("byteCount < 0: " + byteCount);
      if (closed) throw new IllegalStateException("stream closed");
      if (bytesRemaining == 0) return -1L;
      long toRead = Math.min(byteCount, bytesRemaining);
      long read = buffer.byteCount() > 0
          ? buffer.read(sink, toRead, deadline)
          : source.read(sink, toRead, deadline);
      if (read == -1) {
        unexpectedEndOfInput(); // The server didn't supply the promised content length.
        throw new ProtocolException("unexpected end of stream");
      }
      bytesRemaining -= read;
      cacheWrite(sink, read);
      if (bytesRemaining == 0) endOfInput();
      return read;
    }

    @Override public void close(Deadline deadline) throws IOException {
      if (closed) return;
      if (bytesRemaining != 0 && !discardStream(engine, this)) unexpectedEndOfInput();
      closed = true;
    }
  }

  private class ChunkedSource extends AbstractHttpSource {
    private static final int NO_CHUNK_YET = -1;
    private int bytesRemainingInChunk = NO_CHUNK_YET;
    private boolean hasMoreChunks = true;

    ChunkedSource(HttpEngine httpEngine, CacheRequest cacheRequest) throws IOException {
      super(httpEngine, cacheRequest);
    }

    @Override public long read(OkBuffer sink, long byteCount, Deadline deadline)
        throws IOException {
      if (byteCount < 0) throw new IllegalArgumentException("byteCount < 0: " + byteCount);
      if (closed) throw new IllegalStateException("stream closed");
      if (!hasMoreChunks) return -1;

      if (bytesRemainingInChunk == 0 || bytesRemainingInChunk == NO_CHUNK_YET) {
        readChunkSize();
        if (!hasMoreChunks) return -1;
      }

      long toRead = Math.min(byteCount, bytesRemainingInChunk);
      long read = buffer.byteCount() > 0
          ? buffer.read(sink, toRead, deadline)
          : source.read(sink, toRead, deadline);
      if (read == -1) {
        unexpectedEndOfInput(); // The server didn't supply the promised chunk length.
        throw new IOException("unexpected end of stream");
      }
      bytesRemainingInChunk -= read;
      cacheWrite(sink, read);
      return read;
    }

    private void readChunkSize() throws IOException {
      // read the suffix of the previous chunk
      if (bytesRemainingInChunk != NO_CHUNK_YET) {
        readAsciiLine();
      }
      String chunkSizeString = readAsciiLine();
      int index = chunkSizeString.indexOf(";");
      if (index != -1) {
        chunkSizeString = chunkSizeString.substring(0, index);
      }
      try {
        bytesRemainingInChunk = Integer.parseInt(chunkSizeString.trim(), 16);
      } catch (NumberFormatException e) {
        throw new ProtocolException("Expected a hex chunk size but was " + chunkSizeString);
      }
      if (bytesRemainingInChunk == 0) {
        hasMoreChunks = false;
        Headers.Builder trailersBuilder = new Headers.Builder();
        readHeaders(trailersBuilder);
        engine.receiveHeaders(trailersBuilder.build());
        endOfInput();
      }
    }

    @Override public void close(Deadline deadline) throws IOException {
      if (closed) return;
      if (hasMoreChunks && !discardStream(engine, this)) unexpectedEndOfInput();
      closed = true;
    }
  }

  private class UnknownLengthSource extends AbstractHttpSource {
    private boolean inputExhausted;

    UnknownLengthSource(HttpEngine httpEngine, CacheRequest cacheRequest) throws IOException {
      super(httpEngine, cacheRequest);
    }

    @Override public long read(OkBuffer sink, long byteCount, Deadline deadline)
        throws IOException {
      if (byteCount < 0) throw new IllegalArgumentException("byteCount < 0: " + byteCount);
      if (closed) throw new IllegalStateException("stream closed");
      if (inputExhausted) return -1;

      long read = buffer.byteCount() > 0
          ? buffer.read(sink, byteCount, deadline)
          : source.read(sink, byteCount, deadline);
      if (read == -1) {
        inputExhausted = true;
        endOfInput();
        return -1;
      }
      cacheWrite(sink, read);
      return read;
    }

    @Override public void close(Deadline deadline) throws IOException {
      if (closed) return;
      closed = true;
      if (!inputExhausted) unexpectedEndOfInput();
    }
  }
}
