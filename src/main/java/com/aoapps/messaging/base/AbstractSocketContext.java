/*
 * ao-messaging-base - Asynchronous bidirectional messaging over various protocols base for implementations.
 * Copyright (C) 2014, 2015, 2016, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
 * along with ao-messaging-base.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoapps.messaging.base;

import com.aoapps.collections.AoCollections;
import com.aoapps.concurrent.ConcurrentListenerManager;
import com.aoapps.messaging.SocketContext;
import com.aoapps.messaging.SocketContextListener;
import com.aoapps.security.Identifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base implementation of socket context.
 */
public abstract class AbstractSocketContext<S extends AbstractSocket> implements SocketContext {

	private static final Logger logger = Logger.getLogger(AbstractSocketContext.class.getName());

	private final Map<Identifier, S> sockets = new LinkedHashMap<>();

	private final Object closeLock = new Object();
	private boolean closed;

	private final ConcurrentListenerManager<SocketContextListener> listenerManager = new ConcurrentListenerManager<>();

	protected AbstractSocketContext() {
		// Do nothing
	}

	@Override
	public Map<Identifier, S> getSockets() {
		synchronized(sockets) {
			return AoCollections.unmodifiableCopyMap(sockets);
		}
	}

	@Override
	public S getSocket(Identifier id) {
		synchronized(sockets) {
			return sockets.get(id);
		}
	}

	/**
	 * Something that can check the availability of a session ID, to skip used session IDs when
	 * creating new sessions.
	 */
	protected static interface IdentifierAvailabilityChecker {
		boolean isIdentifierAvailable(Identifier id);
	}

	/**
	 * All of the registered session identifier checkers.
	 */
	private final List<IdentifierAvailabilityChecker> idCheckers = new ArrayList<>();

	/**
	 * Adds a {@link IdentifierAvailabilityChecker}.
	 */
	protected void addIdentifierAvailabilityChecker(IdentifierAvailabilityChecker idChecker) {
		synchronized(idCheckers) {
			idCheckers.add(idChecker);
		}
	}

	/**
	 * Removes a {@link IdentifierAvailabilityChecker}.
	 */
	protected void removeIdentifierAvailabilityChecker(IdentifierAvailabilityChecker idChecker) {
		synchronized(idCheckers) {
			idCheckers.remove(idChecker);
		}
	}

	/**
	 * Creates a random identifier that is not in the current set of sockets.
	 * <p>
	 * The identifier is checked against any registered {@link IdentifierAvailabilityChecker}.
	 * </p>
	 * @see  #addIdentifierAvailabilityChecker(com.aoapps.messaging.base.AbstractSocketContext.IdentifierAvailabilityChecker)
	 */
	protected Identifier newIdentifier() {
		while(true) {
			Identifier id = new Identifier();
			boolean available;
			synchronized(sockets) {
				available = !sockets.containsKey(id);
			}
			if(available) {
				synchronized(idCheckers) {
					for(IdentifierAvailabilityChecker idChecker : idCheckers) {
						if(!idChecker.isIdentifierAvailable(id)) {
							available = false;
							break;
						}
					}
				}
				if(available) return id;
			}
		}
	}

	/**
	 * Called by a socket when it is closed.
	 * This will only be called once.
	 */
	void onClose(AbstractSocket socket) {
		synchronized(sockets) {
			if(sockets.remove(socket.getId()) == null) throw new AssertionError("Socket not part of this context.  onClose called twice?");
		}
	}

	/**
	 * Any overriding implementation must call super.close() first.
	 */
	@Override
	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
	public void close() {
		boolean enqueueOnSocketContextClose;
		synchronized(closeLock) {
			if(!closed) {
				closed = true;
				enqueueOnSocketContextClose = true;
			} else {
				enqueueOnSocketContextClose = false;
			}
		}
		List<S> socketsToClose;
		synchronized(sockets) {
			// Gets a copy of the sockets to avoid concurrent modification exception and avoid holding lock
			socketsToClose = new ArrayList<>(sockets.values());
			// Each will be removed from socket.close() below: sockets.clear();
		}
		for(S socket : socketsToClose) {
			try {
				socket.close();
			} catch(ThreadDeath td) {
				throw td;
			} catch(Throwable t) {
				logger.log(Level.SEVERE, null, t);
			}
		}
		if(enqueueOnSocketContextClose) {
			logger.log(Level.FINE, "Enqueuing onSocketContextClose: {0}", this);
			Future<?> future = listenerManager.enqueueEvent(
				listener -> () -> listener.onSocketContextClose(this)
			);
			try {
				logger.log(Level.FINER, "Waiting for onSocketContextClose: {0}", this);
				future.get();
				logger.log(Level.FINER, "Finished onSocketContextClose: {0}", this);
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
		synchronized(sockets) {
			if(!sockets.isEmpty()) throw new AssertionError("Not all sockets closed");
		}
	}

	@Override
	public boolean isClosed() {
		synchronized(closeLock) {
			return closed;
		}
	}

	@Override
	public void addSocketContextListener(SocketContextListener listener, boolean synchronous) throws IllegalStateException {
		listenerManager.addListener(listener, synchronous);
	}

	@Override
	public boolean removeSocketContextListener(SocketContextListener listener) {
		return listenerManager.removeListener(listener);
	}

	/**
	 * Adds a new socket to this context, sockets must be added to the context
	 * before they create any of their own events.  This gives context listeners
	 * a chance to register per-socket listeners in "onNewSocket".
	 *
	 * First, adds to the list of sockets.
	 * Second, calls all listeners notifying them of new socket.
	 * Third, waits for all listeners to handle the event before returning.
	 */
	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
	protected void addSocket(final S newSocket) {
		if(isClosed()) throw new IllegalStateException("SocketContext is closed");
		logger.log(Level.FINE, "Enqueuing onNewSocket: {0}, {1}", new Object[]{this, newSocket});
		Future<?> future;
		synchronized(sockets) {
			Identifier id = newSocket.getId();
			if(sockets.containsKey(id)) throw new IllegalStateException("Socket with the same ID has already been added");
			sockets.put(id, newSocket);
			future = listenerManager.enqueueEvent(
				listener -> () -> listener.onNewSocket(this, newSocket)
			);
		}
		try {
			logger.log(Level.FINER, "Waiting for onNewSocket: {0}, {1}", new Object[]{this, newSocket});
			future.get();
			logger.log(Level.FINER, "Finished onNewSocket: {0}, {1}", new Object[]{this, newSocket});
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
			listener -> () -> listener.onError(this, t)
		);
	}
}
