use futures::{stream::FuturesOrdered, FutureExt, Stream};
use pin_project_lite::pin_project;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::select;

pin_project! {
    /// Future for the [`join_n`] function.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub(crate) struct JoinInto<K, F: Future, U: FnMut(K, F::Output)> {
        #[pin]
        active: FuturesOrdered<KeyedFuture<K, F>>,
        into: U,
    }
}

pin_project! {
    struct KeyedFuture<K, F> {
        #[pin]
        future: F,
        key: Option<K>,
    }
}

impl<K, F: Future> Future for KeyedFuture<K, F> {
    type Output = (K, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let result = ready!(this.future.as_mut().poll(cx));
        Poll::Ready((this.key.take().unwrap(), result))
    }
}

pub(crate) fn join_into<K, F, I: IntoIterator<Item = (K, F)>, U: FnMut(K, F::Output)>(
    iter: I,
    into: U,
) -> JoinInto<K, F, U>
where
    F: Future,
{
    let mut active = FuturesOrdered::default();
    for (key, future) in iter {
        active.push_back(KeyedFuture {
            key: Some(key),
            future,
        });
    }

    JoinUntil {
        active,
        until: into,
    }
}

impl<K: Eq + Hash, F, U: FnMut(K, F::Output)> Future for JoinInto<K, F, U>
where
    F: Future,
{
    type Output = HashMap<K, F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut elapsed = false;
        let mut this = self.project();
        loop {
            match this.active.as_mut().poll_next(cx) {
                Poll::Ready(Some((k, x))) => {
                    (this.into)(k, x);
                }
                Poll::Ready(None) => {
                    // Done with all futures
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
