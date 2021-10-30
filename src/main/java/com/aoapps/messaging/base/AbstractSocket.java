/*
 * ao-messaging-base - Asynchronous bidirectional messaging over various protocols base for implementations.
 * Copyright (C) 2014, 2015, 2016, 2019, 2020, 2021  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-messaging-base.
 *
 * ao-messaging-base is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-messaging-base is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-messaging-base.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoapps.messaging.base;

import com.aoapps.concurrent.Callback;
import com.aoapps.concurrent.ConcurrentListenerManager;
import com.aoapps.messaging.Message;
import com.aoapps.messaging.Socket;
import com.aoapps.messaging.SocketListener;
import com.aoapps.security.Identifier;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base implementation of socket.
 */
public abstract class AbstractSocket implements Socket {

	private static final Logger logger = Logger.getLogger(AbstractSocket.class.getName());

	private final AbstractSocketContext<? extends AbstractSocket> socketContext;

	private final Identifier id;

	private final long connectTime;

	private final    SocketAddress connectRemoteSocketAddress;
	private final    Object        remoteSocketAddressLock = new Object();
	private          SocketAddress remoteSocketAddress;

	private final Object closeLock = new Object();
	private Long closeTime;

	private final ConcurrentListenerManager<SocketListener> listenerManager = new ConcurrentListenerManager<>();

	protected AbstractSocket(
		AbstractSocketContext<? extends AbstractSocket> socketContext,
		Identifier id,
		long connectTime,
		SocketAddress remoteSocketAddress
	) {
		this.socketContext              = socketContext;
		this.id                         = id;
		this.connectTime                = connectTime;
		this.connectRemoteSocketAddress = remoteSocketAddress;
		this.remoteSocketAddress        = remoteSocketAddress;
	}

	@Override
	public String toString() {
		return getRemoteSocketAddress().toString();
	}

	@Override
	public AbstractSocketContext<? extends AbstractSocket> getSocketContext() {
		return socketContext;
	}

	@Override
	public Identifier getId() {
		return id;
	}

	@Override
	public long getConnectTime() {
		return connectTime;
	}

	@Override
	public Long getCloseTime() {
		synchronized(closeLock) {
			return closeTime;
		}
	}

	@Override
	public SocketAddress getConnectRemoteSocketAddress() {
		return connectRemoteSocketAddress;
	}

	@Override
	public SocketAddress getRemoteSocketAddress() {
		synchronized(remoteSocketAddressLock) {
			return remoteSocketAddress;
		}
	}

	/**
	 * Sets the most recently seen remote address.
	 * If the provided value is different than the previous, will notify all listeners.
	 */
	protected void setRemoteSocketAddress(final SocketAddress newRemoteSocketAddress) {
		synchronized(remoteSocketAddressLock) {
			final SocketAddress oldRemoteSocketAddress = this.remoteSocketAddress;
			if(!newRemoteSocketAddress.equals(oldRemoteSocketAddress)) {
				this.remoteSocketAddress = newRemoteSocketAddress;
				logger.log(Level.FINE, "Enqueuing onRemoteSocketAddressChange: {0}, {1}, {2}", new Object[]{this, oldRemoteSocketAddress, newRemoteSocketAddress});
				listenerManager.enqueueEvent(
					(SocketListener listener) -> () -> listener.onRemoteSocketAddressChange(this, oldRemoteSocketAddress, newRemoteSocketAddress)
				);
			}
		}
	}

	/**
	 * Makes sure the socket is not already closed then calls startImpl.
	 *
	 * @see  #startImpl(com.aoapps.concurrent.Callback, com.aoapps.concurrent.Callback)
	 */
	@Override
	public void start(
		Callback<? super Socket> onStart,
		Callback<? super Throwable> onError
	) throws IllegalStateException {
		if(isClosed()) throw new IllegalStateException("Socket is closed");
		startImpl(onStart, onError);
	}

	/**
	 * Any overriding implementation must call super.close() first.
	 */
	@Override
	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
	public void close() throws IOException {
		boolean enqueueOnSocketClose;
		synchronized(closeLock) {
			if(closeTime == null) {
				// Remove this from the context
				logger.log(Level.FINE, "Calling onClose: {0}", this);
				socketContext.onClose(this);
				closeTime = System.currentTimeMillis();
				enqueueOnSocketClose = true;
			} else {
				enqueueOnSocketClose = false;
			}
		}
		if(enqueueOnSocketClose) {
			logger.log(Level.FINE, "Enqueuing onSocketClose: {0}", this);
			Future<?> future = listenerManager.enqueueEvent(
				(SocketListener listener) -> () -> listener.onSocketClose(this)
			);
			try {
				logger.log(Level.FINER, "Waiting for onSocketClose: {0}", this);
				future.get();
				logger.log(Level.FINER, "Finished onSocketClose: {0}", this);
			} catch(InterruptedException e) {
				logger.log(Level.FINE, null, e);
				// Restore the interrupted status
				Thread.currentThread().interrupt();
			} catch(ThreadDeath td) {
				throw td;
			} catch(Throwable t) {
				logger.log(Level.SEVERE, null, t);
			}
		}
		listenerManager.close();
	}

	@Override
	public boolean isClosed() {
		return getCloseTime() != null;
	}

	@Override
	public void addSocketListener(SocketListener listener, boolean synchronous) throws IllegalStateException {
		listenerManager.addListener(listener, synchronous);
	}

	@Override
	public boolean removeSocketListener(SocketListener listener) {
		return listenerManager.removeListener(listener);
	}

	/**
	 * When one or more new messages have arrived, call this to distribute to all of the listeners.
	 * If need to wait until all of the listeners have handled the messages, can call Future.get()
	 * or Future.isDone().
	 *
	 * @throws  IllegalStateException  if this socket is closed
	 */
	protected Future<?> callOnMessages(final List<? extends Message> messages) throws IllegalStateException {
		if(isClosed()) throw new IllegalStateException("Socket is closed");
		int size = messages.size();
		if(size == 0) throw new IllegalArgumentException("messages may not be empty");
		logger.log(Level.FINE, "Enqueuing onMessages: {0}, {1} {2}", new Object[]{this, size, (size == 1) ? "message" : "messages"});
		return listenerManager.enqueueEvent(
			(SocketListener listener) -> () -> listener.onMessages(this, messages)
		);
	}

	/**
	 * When an error as occurred, call this to distribute to all of the listeners.
	 * If need to wait until all of the listeners have handled the error, can call Future.get()
	 * or Future.isDone().
	 *
	 * @throws  IllegalStateException  if this socket is closed
	 */
	protected Future<?> callOnError(Throwable t) throws IllegalStateException {
		if(isClosed()) throw new IllegalStateException("Socket is closed");
		logger.log(Level.FINE, t, () -> "Enqueuing onError: " + this);
		return listenerManager.enqueueEvent(
			(SocketListener listener) -> () -> listener.onError(this, t)
		);
	}

	@Override
	public void sendMessage(Message message) throws IllegalStateException {
		if(isClosed()) throw new IllegalStateException("Socket is closed");
		sendMessages(Collections.singletonList(message));
	}

	@Override
	public void sendMessages(Collection<? extends Message> messages) throws IllegalStateException {
		if(isClosed()) throw new IllegalStateException("Socket is closed");
		logger.log(Level.FINEST, "messages = {0}", messages);
		if(!messages.isEmpty()) sendMessagesImpl(messages);
	}

	@Override
	public abstract String getProtocol();

	/**
	 * Called once the socket is confirmed to not be closed.
	 *
	 * @see  #start(com.aoapps.concurrent.Callback, com.aoapps.concurrent.Callback)
	 *
	 * @throws IllegalStateException  if already started
	 */
	protected abstract void startImpl(
		Callback<? super Socket> onStart,
		Callback<? super Throwable> onError
	) throws IllegalStateException;

	/**
	 * Implementation to actually enqueue and send messages.
	 * This must never block.
	 */
	protected abstract void sendMessagesImpl(Collection<? extends Message> messages);
}
