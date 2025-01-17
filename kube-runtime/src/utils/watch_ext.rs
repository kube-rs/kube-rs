use crate::{
    utils::{
        event_decode::EventDecode,
        event_modify::EventModify,
        predicate::{Predicate, PredicateFilter},
        stream_backoff::StreamBackoff,
    },
    watcher,
};
use kube_client::Resource;

use crate::{
    reflector::store::Writer,
    utils::{Backoff, Reflect},
};

use crate::watcher::DefaultBackoff;
use futures::{Stream, TryStream};

/// Extension trait for streams returned by [`watcher`](watcher()) or [`reflector`](crate::reflector::reflector)
pub trait WatchStreamExt: Stream {
    /// Apply the [`DefaultBackoff`] watcher [`Backoff`] policy
    ///
    /// This is recommended for controllers that want to play nicely with the apiserver.
    fn default_backoff(self) -> StreamBackoff<Self, DefaultBackoff>
    where
        Self: TryStream + Sized,
    {
        StreamBackoff::new(self, DefaultBackoff::default())
    }

    /// Apply a specific [`Backoff`] policy to a [`Stream`] using [`StreamBackoff`]
    fn backoff<B>(self, b: B) -> StreamBackoff<Self, B>
    where
        B: Backoff,
        Self: TryStream + Sized,
    {
        StreamBackoff::new(self, b)
    }

    /// Decode a [`watcher()`] stream into a stream of applied objects
    ///
    /// All Added/Modified events are passed through, and critical errors bubble up.
    fn applied_objects<K>(self) -> EventDecode<Self>
    where
        Self: Stream<Item = Result<watcher::Event<K>, watcher::Error>> + Sized,
    {
        EventDecode::new(self, false)
    }

    /// Decode a [`watcher()`] stream into a stream of touched objects
    ///
    /// All Added/Modified/Deleted events are passed through, and critical errors bubble up.
    fn touched_objects<K>(self) -> EventDecode<Self>
    where
        Self: Stream<Item = Result<watcher::Event<K>, watcher::Error>> + Sized,
    {
        EventDecode::new(self, true)
    }

    /// Modify elements of a [`watcher()`] stream.
    ///
    /// Calls [`watcher::Event::modify()`] on every element.
    /// Stream shorthand for `stream.map_ok(|event| { event.modify(f) })`.
    ///
    /// ```no_run
    /// # use std::pin::pin;
    /// # use futures::{Stream, StreamExt, TryStreamExt};
    /// # use kube::{Api, Client, ResourceExt};
    /// # use kube_runtime::{watcher, WatchStreamExt};
    /// # use k8s_openapi::api::apps::v1::Deployment;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let deploys: Api<Deployment> = Api::all(client);
    /// let mut truncated_deploy_stream = pin!(watcher(deploys, watcher::Config::default())
    ///     .modify(|deploy| {
    ///         deploy.managed_fields_mut().clear();
    ///         deploy.status = None;
    ///     })
    ///     .applied_objects());
    ///
    /// while let Some(d) = truncated_deploy_stream.try_next().await? {
    ///    println!("Truncated Deployment: '{:?}'", serde_json::to_string(&d)?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn modify<F, K>(self, f: F) -> EventModify<Self, F>
    where
        Self: Stream<Item = Result<watcher::Event<K>, watcher::Error>> + Sized,
        F: FnMut(&mut K),
    {
        EventModify::new(self, f)
    }

    /// Filter a stream based on on [`predicates`](crate::predicates).
    ///
    /// This will filter out repeat calls where the predicate returns the same result.
    /// Common use case for this is to avoid repeat events for status updates
    /// by filtering on [`predicates::generation`](crate::predicates::generation).
    ///
    /// ## Usage
    /// ```no_run
    /// # use std::pin::pin;
    /// # use futures::{Stream, StreamExt, TryStreamExt};
    /// use kube::{Api, Client, ResourceExt};
    /// use kube_runtime::{watcher, WatchStreamExt, predicates};
    /// use k8s_openapi::api::apps::v1::Deployment;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let deploys: Api<Deployment> = Api::default_namespaced(client);
    /// let mut changed_deploys = pin!(watcher(deploys, watcher::Config::default())
    ///     .applied_objects()
    ///     .predicate_filter(predicates::generation));
    ///
    /// while let Some(d) = changed_deploys.try_next().await? {
    ///    println!("saw Deployment '{} with hitherto unseen generation", d.name_any());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn predicate_filter<K, P>(self, predicate: P) -> PredicateFilter<Self, K, P>
    where
        Self: Stream<Item = Result<K, watcher::Error>> + Sized,
        K: Resource + 'static,
        P: Predicate<K> + 'static,
    {
        PredicateFilter::new(self, predicate)
    }

    /// Reflect a [`watcher()`] stream into a [`Store`] through a [`Writer`]
    ///
    /// Returns the stream unmodified, but passes every [`watcher::Event`] through a [`Writer`].
    /// This populates a [`Store`] as the stream is polled.
    ///
    /// ## Usage
    /// ```no_run
    /// # use futures::{Stream, StreamExt, TryStreamExt};
    /// # use std::time::Duration;
    /// # use tracing::{info, warn};
    /// use kube::{Api, Client, ResourceExt};
    /// use kube_runtime::{watcher, WatchStreamExt, reflector};
    /// use k8s_openapi::api::apps::v1::Deployment;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    ///
    /// let deploys: Api<Deployment> = Api::default_namespaced(client);
    /// let (reader, writer) = reflector::store::<Deployment>();
    ///
    /// tokio::spawn(async move {
    ///     // start polling the store once the reader is ready
    ///     reader.wait_until_ready().await.unwrap();
    ///     loop {
    ///         let names = reader.state().iter().map(|d| d.name_any()).collect::<Vec<_>>();
    ///         info!("Current {} deploys: {:?}", names.len(), names);
    ///         tokio::time::sleep(Duration::from_secs(10)).await;
    ///     }
    /// });
    ///
    /// // configure the watcher stream and populate the store while polling
    /// watcher(deploys, watcher::Config::default())
    ///     .reflect(writer)
    ///     .applied_objects()
    ///     .for_each(|res| async move {
    ///         match res {
    ///             Ok(o) => info!("saw {}", o.name_any()),
    ///             Err(e) => warn!("watcher error: {}", e),
    ///         }
    ///     })
    ///     .await;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Store`]: crate::reflector::Store
    fn reflect<K>(self, writer: Writer<K>) -> Reflect<Self, K>
    where
        Self: Stream<Item = watcher::Result<watcher::Event<K>>> + Sized,
        K: Resource + Clone + 'static,
        K::DynamicType: Eq + std::hash::Hash + Clone,
    {
        Reflect::new(self, writer)
    }

    /// Reflect a shared [`watcher()`] stream into a [`Store`] through a [`Writer`]
    ///
    /// Returns the stream unmodified, but passes every [`watcher::Event`]
    /// through a [`Writer`]. This populates a [`Store`] as the stream is
    /// polled. When the [`watcher::Event`] is not an error or a
    /// [`watcher::Event::Deleted`] then its inner object will also be
    /// propagated to subscribers.
    ///
    /// Subscribers can be created by calling [`subscribe()`] on a [`Writer`].
    /// This will return a [`ReflectHandle`] stream that should be polled
    /// independently. When the root stream is dropped, or it ends, all [`ReflectHandle`]s
    /// subscribed to the stream will also terminate after all events yielded by
    /// the root stream have been observed. This means [`ReflectHandle`] streams
    /// can still be polled after the root stream has been dropped.
    ///
    /// **NB**: This adapter requires an
    /// [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21)
    /// feature
    ///
    /// ## Warning
    ///
    /// If the root [`Stream`] is not polled, [`ReflectHandle`] streams will
    /// never receive any events. This will cause the streams to deadlock since
    /// the root stream will apply backpressure when downstream readers are not
    /// consuming events.
    ///
    ///
    /// [`Store`]: crate::reflector::Store
    /// [`subscribe()`]: crate::reflector::store::Writer::subscribe()
    /// [`Stream`]: futures::stream::Stream
    /// [`ReflectHandle`]: crate::reflector::dispatcher::ReflectHandle
    /// ## Usage
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use std::time::Duration;
    /// # use tracing::{info, warn};
    /// use kube::{Api, ResourceExt};
    /// use kube_runtime::{watcher, WatchStreamExt, reflector};
    /// use k8s_openapi::api::apps::v1::Deployment;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    ///
    /// let deploys: Api<Deployment> = Api::default_namespaced(client);
    /// let subscriber_buf_sz = 100;
    /// let (reader, writer) = reflector::store_shared::<Deployment>(subscriber_buf_sz);
    /// let subscriber = writer.subscribe().unwrap();
    ///
    /// tokio::spawn(async move {
    ///     // start polling the store once the reader is ready
    ///     reader.wait_until_ready().await.unwrap();
    ///     loop {
    ///         let names = reader.state().iter().map(|d| d.name_any()).collect::<Vec<_>>();
    ///         info!("Current {} deploys: {:?}", names.len(), names);
    ///         tokio::time::sleep(Duration::from_secs(10)).await;
    ///     }
    /// });
    ///
    /// tokio::spawn(async move {
    ///     // subscriber can be used to receive applied_objects
    ///     subscriber.for_each(|obj| async move {
    ///         info!("saw in subscriber {}", &obj.name_any())
    ///     }).await;
    /// });
    ///
    /// // configure the watcher stream and populate the store while polling
    /// watcher(deploys, watcher::Config::default())
    ///     .reflect_shared(writer)
    ///     .applied_objects()
    ///     .for_each(|res| async move {
    ///         match res {
    ///             Ok(o) => info!("saw in root stream {}", o.name_any()),
    ///             Err(e) => warn!("watcher error in root stream: {}", e),
    ///         }
    ///     })
    ///     .await;
    ///
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "unstable-runtime-subscribe")]
    fn reflect_shared<K>(self, writer: Writer<K>) -> impl Stream<Item = Self::Item>
    where
        Self: Stream<Item = watcher::Result<watcher::Event<K>>> + Sized,
        K: Resource + Clone + 'static,
        K::DynamicType: Eq + std::hash::Hash + Clone,
    {
        crate::reflector(writer, self)
    }
}

impl<St: ?Sized> WatchStreamExt for St where St: Stream {}

// Compile tests
#[cfg(test)]
pub(crate) mod tests {
    use super::watcher;
    use crate::{predicates, WatchStreamExt as _};
    use futures::prelude::*;
    use k8s_openapi::api::core::v1::Pod;
    use kube_client::{Api, Resource};

    fn compile_type<T>() -> T {
        unimplemented!("not called - compile test only")
    }

    pub fn assert_stream<T, K>(x: T) -> T
    where
        T: Stream<Item = watcher::Result<K>> + Send,
        K: Resource + Clone + Send + 'static,
    {
        x
    }

    // not #[test] because this is only a compile check verification
    #[allow(dead_code, unused_must_use)]
    fn test_watcher_stream_type_drift() {
        let pred_watch = watcher(compile_type::<Api<Pod>>(), Default::default())
            .touched_objects()
            .predicate_filter(predicates::generation)
            .boxed();
        assert_stream(pred_watch);
    }
}
