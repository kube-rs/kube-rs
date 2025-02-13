use core::{
    pin::Pin,
    task::{Context, Poll},
};
use std::marker::PhantomData;

use futures::{Stream, TryStream};
use pin_project::pin_project;

use crate::{
    reflector::store::CacheWriter,
    watcher::{Error, Event},
};
use kube_client::Resource;

/// Stream returned by the [`reflect`](super::WatchStreamExt::reflect) method
#[pin_project]
pub struct Reflect<St, K, W>
where
    K: Resource + Clone + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone,
    W: CacheWriter<K>,
{
    #[pin]
    stream: St,
    writer: W,
    _phantom: PhantomData<K>,
}

impl<St, K, W> Reflect<St, K, W>
where
    St: TryStream<Ok = Event<K>>,
    K: Resource + Clone,
    K::DynamicType: Eq + std::hash::Hash + Clone,
    W: CacheWriter<K>,
{
    pub(super) fn new(stream: St, writer: W) -> Reflect<St, K, W> {
        Self {
            stream,
            writer,
            _phantom: Default::default(),
        }
    }
}

impl<St, K, W> Stream for Reflect<St, K, W>
where
    K: Resource + Clone,
    K::DynamicType: Eq + std::hash::Hash + Clone,
    St: Stream<Item = Result<Event<K>, Error>>,
    W: CacheWriter<K>,
{
    type Item = Result<Event<K>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        me.stream.as_mut().poll_next(cx).map_ok(move |event| {
            me.writer.apply_watcher_event(&event);
            event
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{pin::pin, task::Poll};

    use super::{Error, Event, Reflect};
    use crate::reflector;
    use futures::{poll, stream, StreamExt};
    use k8s_openapi::api::core::v1::Pod;

    fn testpod(name: &str) -> Pod {
        let mut pod = Pod::default();
        pod.metadata.name = Some(name.to_string());
        pod
    }

    #[tokio::test]
    async fn reflect_passes_events_through() {
        let foo = testpod("foo");
        let bar = testpod("bar");
        let st = stream::iter([
            Ok(Event::Apply(foo.clone())),
            Err(Error::NoResourceVersion),
            Ok(Event::Init),
            Ok(Event::InitApply(foo)),
            Ok(Event::InitApply(bar)),
            Ok(Event::InitDone),
        ]);
        let (reader, writer) = reflector::store();

        let mut reflect = pin!(Reflect::new(st, writer));
        assert_eq!(reader.len(), 0);

        assert!(matches!(
            poll!(reflect.next()),
            Poll::Ready(Some(Ok(Event::Apply(_))))
        ));
        assert_eq!(reader.len(), 1);

        assert!(matches!(
            poll!(reflect.next()),
            Poll::Ready(Some(Err(Error::NoResourceVersion)))
        ));
        assert_eq!(reader.len(), 1);

        assert!(matches!(
            poll!(reflect.next()),
            Poll::Ready(Some(Ok(Event::Init)))
        ));
        assert_eq!(reader.len(), 1);

        let restarted = poll!(reflect.next());
        assert!(matches!(restarted, Poll::Ready(Some(Ok(Event::InitApply(_))))));
        assert_eq!(reader.len(), 1);
        let restarted = poll!(reflect.next());
        assert!(matches!(restarted, Poll::Ready(Some(Ok(Event::InitApply(_))))));
        assert_eq!(reader.len(), 1);

        assert!(matches!(
            poll!(reflect.next()),
            Poll::Ready(Some(Ok(Event::InitDone)))
        ));
        assert_eq!(reader.len(), 2);

        assert!(matches!(poll!(reflect.next()), Poll::Ready(None)));
        assert_eq!(reader.len(), 2);
    }
}
