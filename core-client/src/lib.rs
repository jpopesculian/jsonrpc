//! JSON-RPC client implementation.
#![deny(missing_docs)]

use failure::{format_err, Fail};
use futures::sync::{mpsc, oneshot};
use futures::{future, prelude::*};
use jsonrpc_core::{Call, Error, Id, MethodCall, Output, Params, Request, Response, Version};
use log::debug;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::VecDeque;
use tokio;

/// The errors returned by the client.
#[derive(Debug, Fail)]
pub enum RpcError {
	/// An error returned by the server.
	#[fail(display = "Server returned rpc error {}", _0)]
	JsonRpcError(Error),
	/// Failure to parse server response.
	#[fail(display = "Failed to parse server response as {}: {}", _0, _1)]
	ParseError(String, failure::Error),
	/// Request timed out.
	#[fail(display = "Request timed out")]
	Timeout,
	/// The server returned a response with an unknown id.
	#[fail(display = "Server returned a response with an unknown id")]
	UnknownId,
	/// Not rpc specific errors.
	#[fail(display = "{}", _0)]
	Other(failure::Error),
}

impl From<Error> for RpcError {
	fn from(error: Error) -> Self {
		RpcError::JsonRpcError(error)
	}
}

/// The future retured by the client.
pub struct RpcFuture<T> {
	recv: oneshot::Receiver<Result<T, Error>>,
}

impl<T> RpcFuture<T> {
	/// Creates a new `RpcFuture`.
	pub fn new(recv: oneshot::Receiver<Result<T, Error>>) -> Self {
		RpcFuture { recv }
	}
}

impl<T> Future for RpcFuture<T> {
	type Item = T;
	type Error = RpcError;

	fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
		// TODO should timeout (#410)
		match self.recv.poll() {
			Ok(Async::Ready(Ok(value))) => Ok(Async::Ready(value)),
			Ok(Async::Ready(Err(error))) => Err(RpcError::JsonRpcError(error)),
			Ok(Async::NotReady) => Ok(Async::NotReady),
			Err(error) => Err(RpcError::Other(error.into())),
		}
	}
}

/// Stream wrapper for subscription
pub struct RpcSubscription<T, S, F>
where
	S: Stream<Item = T, Error = RpcError>,
	F: FnOnce(),
{
	stream: Option<S>,
	unsubscribe: Option<F>,
}

impl<T, S, F> RpcSubscription<T, S, F>
where
	S: Stream<Item = T, Error = RpcError>,
	F: FnOnce(),
{
	/// Construct new subscription
	pub fn new(receiver: S, unsubscribe: F) -> Self {
		Self {
			stream: Some(receiver),
			unsubscribe: Some(unsubscribe),
		}
	}

	/// Get underlying stream
	pub fn stream(&mut self) -> Option<S> {
		std::mem::replace(&mut self.stream, None)
	}
}

impl<T, S, F> Drop for RpcSubscription<T, S, F>
where
	S: Stream<Item = T, Error = RpcError>,
	F: FnOnce(),
{
	fn drop(&mut self) {
		if let Some(f) = std::mem::replace(&mut self.unsubscribe, None) {
			f();
		}
	}
}

/// Params for subscription notification
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcNotificationParams {
	/// Payload
	pub result: Value,
	/// Subscription id
	pub subscription: String,
}

/// Notification from subscriptions
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcNotification {
	/// Protocol version
	pub jsonrpc: Option<Version>,
	/// RPC Method
	pub method: String,
	/// Notification params
	pub params: RpcNotificationParams,
}

impl RpcNotification {
	/// gets a subscription Id for notifications
	pub fn id(&self) -> Id {
		Id::Str(self.params.subscription.clone())
	}
}

impl From<RpcNotification> for Value {
	/// Convert into a result. Will be `Ok` if it is a `Success` and `Err` if `Failure`.
	fn from(notification: RpcNotification) -> Value {
		notification.params.result
	}
}

/// A message sent to the `RpcClient`. This is public so that
/// the derive crate can generate a client.
#[derive(Debug)]
pub enum RpcMessage {
	/// Outgoing message type
	Outgoing(RpcOutgoingMessage),
	/// Subscribe message type
	Subscribe(RpcSubscribeMessage),
	/// Unsubscribe message type
	Unsubscribe(RpcUnsubscribeMessage),
}

/// Subscribe Message
#[derive(Debug)]
pub struct RpcSubscribeMessage {
	id: Id,
	sender: mpsc::Sender<Value>,
	res_sender: oneshot::Sender<Result<(), Error>>,
}

/// Unsubscribe Message
#[derive(Debug)]
pub struct RpcUnsubscribeMessage {
	id: Id,
	res_sender: oneshot::Sender<Result<(), Error>>,
}

/// Outgoing Message
#[derive(Debug)]
pub struct RpcOutgoingMessage {
	/// The rpc method name.
	method: String,
	/// The rpc method parameters.
	params: Params,
	/// The mpsc channel to send the result of the rpc
	/// call to.
	sender: oneshot::Sender<Result<Value, Error>>,
}

/// A channel to a `RpcClient`.
pub type RpcChannel = mpsc::Sender<RpcMessage>;

/// The RpcClient handles sending and receiving asynchronous
/// messages through an underlying transport.
pub struct RpcClient<TSink, TStream> {
	id: u64,
	queue: HashMap<Id, oneshot::Sender<Result<Value, Error>>>,
	subscriptions: HashMap<Id, mpsc::Sender<Value>>,
	sink: TSink,
	stream: TStream,
	channel: Option<mpsc::Receiver<RpcMessage>>,
	outgoing: VecDeque<String>,
}

impl<TSink, TStream> RpcClient<TSink, TStream> {
	/// Creates a new `RpcClient`.
	pub fn new(sink: TSink, stream: TStream, channel: mpsc::Receiver<RpcMessage>) -> Self {
		RpcClient {
			id: 0,
			queue: HashMap::new(),
			subscriptions: HashMap::new(),
			sink,
			stream,
			channel: Some(channel),
			outgoing: VecDeque::new(),
		}
	}

	fn next_id(&mut self) -> Id {
		let id = self.id;
		self.id = id + 1;
		Id::Num(id)
	}
}

impl<TSink, TStream> Future for RpcClient<TSink, TStream>
where
	TSink: Sink<SinkItem = String, SinkError = RpcError>,
	TStream: Stream<Item = String, Error = RpcError>,
{
	type Item = ();
	type Error = RpcError;

	fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
		// Handle requests from the client.
		loop {
			if self.channel.is_none() {
				break;
			}
			let msg = match self.channel.as_mut().expect("channel is some; qed").poll() {
				Ok(Async::Ready(Some(msg))) => msg,
				Ok(Async::Ready(None)) => {
					// When the channel is dropped we still need to finish
					// outstanding requests.
					self.channel.take();
					break;
				}
				Ok(Async::NotReady) => break,
				Err(()) => continue,
			};
			let id = self.next_id();
			match msg {
				RpcMessage::Outgoing(msg) => {
					let request = Request::Single(Call::MethodCall(MethodCall {
						jsonrpc: Some(Version::V2),
						method: msg.method,
						params: msg.params,
						id: id.clone(),
					}));
					self.queue.insert(id, msg.sender);
					let request_str = serde_json::to_string(&request).map_err(|error| RpcError::Other(error.into()))?;
					self.outgoing.push_back(request_str);
				}
				RpcMessage::Subscribe(msg) => {
					// TODO handle overlapping ids
					self.subscriptions.insert(msg.id, msg.sender);
					msg.res_sender
						.send(Ok(()))
						.map_err(|_| RpcError::Other(format_err!("oneshot channel closed")))?;
				}
				RpcMessage::Unsubscribe(msg) => {
					// TODO handle missing ids
					let _ = self.subscriptions.remove(&msg.id);
					msg.res_sender
						.send(Ok(()))
						.map_err(|_| RpcError::Other(format_err!("oneshot channel closed")))?;
				}
			}
		}
		// Handle outgoing rpc requests.
		loop {
			match self.outgoing.pop_front() {
				Some(request) => match self.sink.start_send(request)? {
					AsyncSink::Ready => {}
					AsyncSink::NotReady(request) => {
						self.outgoing.push_front(request);
						break;
					}
				},
				None => break,
			}
		}
		let done_sending = match self.sink.poll_complete()? {
			Async::Ready(()) => true,
			Async::NotReady => false,
		};
		// Handle incoming rpc requests.
		loop {
			let response_str = match self.stream.poll() {
				Ok(Async::Ready(Some(response_str))) => response_str,
				Ok(Async::Ready(None)) => {
					// The websocket connection was closed so the client
					// can be shutdown. Reopening closed connections must
					// be handled by the transport.
					debug!("connection closed");
					return Ok(Async::Ready(()));
				}
				Ok(Async::NotReady) => break,
				Err(err) => Err(err)?,
			};

			if let Ok(response) = serde_json::from_str::<Response>(&response_str) {
				let outputs: Vec<Output> = match response {
					Response::Single(output) => vec![output],
					Response::Batch(outputs) => outputs,
				};
				for output in outputs {
					let channel = self.queue.remove(output.id());
					let value: Result<Value, Error> = output.into();
					match channel {
						Some(tx) => tx
							.send(value)
							.map_err(|_| RpcError::Other(format_err!("oneshot channel closed")))?,
						None => Err(RpcError::UnknownId)?,
					};
				}
			} else if let Ok(response) = serde_json::from_str::<RpcNotification>(&response_str) {
				if let Some(subscription) = self.subscriptions.get_mut(&response.id()) {
					subscription
						.try_send(response.params.result)
						.map_err(|_| RpcError::Other(format_err!("subscription channel closed")))?;
				}
			} else {
				return Err(RpcError::UnknownId);
			}
		}
		if self.channel.is_none()
			&& self.outgoing.is_empty()
			&& self.queue.is_empty()
			&& self.subscriptions.is_empty()
			&& done_sending
		{
			debug!("client finished");
			Ok(Async::Ready(()))
		} else {
			Ok(Async::NotReady)
		}
	}
}

/// Client for raw JSON RPC requests
#[derive(Clone)]
pub struct RawClient(RpcChannel);

impl From<RpcChannel> for RawClient {
	fn from(channel: RpcChannel) -> Self {
		RawClient(channel)
	}
}

impl RawClient {
	/// Call RPC with raw JSON
	pub fn call_method(&self, method: &str, params: Params) -> impl Future<Item = Value, Error = RpcError> {
		let (sender, receiver) = oneshot::channel();
		let msg = RpcMessage::Outgoing(RpcOutgoingMessage {
			method: method.into(),
			params,
			sender,
		});
		self.0
			.to_owned()
			.send(msg)
			.map_err(|error| RpcError::Other(error.into()))
			.and_then(|_| RpcFuture::new(receiver))
	}

	/// Subscribe producer to receive messages with id
	pub fn subscribe(&self, id: Id, sender: mpsc::Sender<Value>) -> impl Future<Item = Id, Error = RpcError> {
		let (res_sender, res_receiver) = oneshot::channel();
		let msg = RpcMessage::Subscribe(RpcSubscribeMessage {
			id: id.clone(),
			sender,
			res_sender,
		});
		self.0
			.to_owned()
			.send(msg)
			.map_err(|error| RpcError::Other(error.into()))
			.and_then(|_| RpcFuture::new(res_receiver))
			.and_then(move |_| future::ok(id))
	}

	/// Subscribe producer to receive messages with id
	pub fn unsubscribe(&self, id: Id) -> impl Future<Item = (), Error = ()> {
		let (res_sender, res_receiver) = oneshot::channel();
		let msg = RpcMessage::Unsubscribe(RpcUnsubscribeMessage {
			id: id.clone(),
			res_sender,
		});
		self.0
			.to_owned()
			.send(msg)
			.map_err(|error| RpcError::Other(error.into()))
			.and_then(|_| RpcFuture::new(res_receiver))
			.map_err(|_| ())
	}
}

/// Client for typed JSON RPC requests
#[derive(Clone)]
pub struct TypedClient(RawClient);

impl From<RpcChannel> for TypedClient {
	fn from(channel: RpcChannel) -> Self {
		TypedClient(channel.into())
	}
}

impl TypedClient {
	/// Create new TypedClient
	pub fn new(raw_cli: RawClient) -> Self {
		TypedClient(raw_cli)
	}

	/// Call RPC with serialization of request and deserialization of response
	pub fn call_method<T: Serialize, R: DeserializeOwned + 'static>(
		&self,
		method: &str,
		returns: &'static str,
		args: T,
	) -> impl Future<Item = R, Error = RpcError> {
		let args =
			serde_json::to_value(args).expect("Only types with infallible serialisation can be used for JSON-RPC");
		let params = match args {
			Value::Array(vec) => Params::Array(vec),
			Value::Null => Params::None,
			_ => {
				return future::Either::A(future::err(RpcError::Other(format_err!(
					"RPC params should serialize to a JSON array, or null"
				))))
			}
		};

		future::Either::B(self.0.call_method(method, params).and_then(move |value: Value| {
			log::debug!("response: {:?}", value);
			let result =
				serde_json::from_value::<R>(value).map_err(|error| RpcError::ParseError(returns.into(), error.into()));
			future::done(result)
		}))
	}

	/// Call RPC with serialization of request and deserialization of response
	pub fn subscribe<T, R>(
		&self,
		method: &str,
		args: T,
		unsubscribe_method: &str,
	) -> impl Future<Item = RpcSubscription<R, impl Stream<Item = R, Error = RpcError>, impl FnOnce()>, Error = RpcError>
	where
		T: Serialize,
		R: DeserializeOwned + 'static,
	{
		let sub_client = self.0.clone();
		let unsub_client = self.0.clone();
		let unsub_method = unsubscribe_method.to_owned();

		let (sender, receiver) = mpsc::channel(0);
		let receiver = receiver.then(|val: Result<Value, ()>| {
			val.map_err(|_| RpcError::UnknownId).and_then(|val| {
				serde_json::from_value::<R>(val)
					.map_err(|error| RpcError::ParseError("stream subscription value".into(), error.into()))
			})
		});

		self.call_method(method, "subscription", args)
			.and_then(move |id: String| {
				let id = Id::Str(id.clone());
				sub_client.subscribe(id, sender)
			})
			.and_then(move |id| {
				let unsubscribe = move || {
					tokio::spawn(
						unsub_client
							.call_method(
								&unsub_method,
								Params::Array(vec![serde_json::to_value(id.clone()).unwrap()]),
							)
							.map(|_| ())
							.map_err(|_| ()),
					);
					tokio::spawn(unsub_client.unsubscribe(id));
				};
				future::ok(RpcSubscription::new(receiver, unsubscribe))
			})
	}
}

/// Rpc client implementation for `Deref<Target=MetaIoHandler<Metadata + Default>>`.
pub mod local {
	use super::*;
	use jsonrpc_core::{MetaIoHandler, Metadata};
	use std::ops::Deref;

	/// Implements a rpc client for `MetaIoHandler`.
	pub struct LocalRpc<THandler> {
		handler: THandler,
		queue: VecDeque<String>,
	}

	impl<TMetadata, THandler> LocalRpc<THandler>
	where
		TMetadata: Metadata + Default,
		THandler: Deref<Target = MetaIoHandler<TMetadata>>,
	{
		/// Creates a new `LocalRpc`.
		pub fn new(handler: THandler) -> Self {
			Self {
				handler,
				queue: VecDeque::new(),
			}
		}
	}

	impl<TMetadata, THandler> Stream for LocalRpc<THandler>
	where
		TMetadata: Metadata + Default,
		THandler: Deref<Target = MetaIoHandler<TMetadata>>,
	{
		type Item = String;
		type Error = RpcError;

		fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
			match self.queue.pop_front() {
				Some(response) => Ok(Async::Ready(Some(response))),
				None => Ok(Async::NotReady),
			}
		}
	}

	impl<TMetadata, THandler> Sink for LocalRpc<THandler>
	where
		TMetadata: Metadata + Default,
		THandler: Deref<Target = MetaIoHandler<TMetadata>>,
	{
		type SinkItem = String;
		type SinkError = RpcError;

		fn start_send(&mut self, request: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
			match self.handler.handle_request_sync(&request, TMetadata::default()) {
				Some(response) => self.queue.push_back(response),
				None => {}
			};
			Ok(AsyncSink::Ready)
		}

		fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
			Ok(Async::Ready(()))
		}
	}

	/// Connects to a `IoHandler`.
	pub fn connect<TClient, TMetadata, THandler>(
		handler: THandler,
	) -> (TClient, impl Future<Item = (), Error = RpcError>)
	where
		TClient: From<RpcChannel>,
		TMetadata: Metadata + Default,
		THandler: Deref<Target = MetaIoHandler<TMetadata>>,
	{
		let (sink, stream) = local::LocalRpc::new(handler).split();
		let (sender, receiver) = mpsc::channel(0);
		let rpc_client = RpcClient::new(sink, stream, receiver);
		let client = TClient::from(sender);
		(client, rpc_client)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{RpcChannel, RpcError, TypedClient};
	use jsonrpc_core::{self, IoHandler};

	#[derive(Clone)]
	struct AddClient(TypedClient);

	impl From<RpcChannel> for AddClient {
		fn from(channel: RpcChannel) -> Self {
			AddClient(channel.into())
		}
	}

	impl AddClient {
		fn add(&self, a: u64, b: u64) -> impl Future<Item = u64, Error = RpcError> {
			self.0.call_method("add", "u64", (a, b))
		}
	}

	#[test]
	fn test_client_terminates() {
		let mut handler = IoHandler::new();
		handler.add_method("add", |params: Params| {
			let (a, b) = params.parse::<(u64, u64)>()?;
			let res = a + b;
			Ok(jsonrpc_core::to_value(res).unwrap())
		});

		let (client, rpc_client) = local::connect::<AddClient, _, _>(handler);
		let fut = client
			.clone()
			.add(3, 4)
			.and_then(move |res| client.add(res, 5))
			.join(rpc_client)
			.map(|(res, ())| {
				assert_eq!(res, 12);
			})
			.map_err(|err| {
				eprintln!("{:?}", err);
				assert!(false);
			});
		tokio::run(fut);
	}
}
