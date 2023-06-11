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
    pub(crate) struct JoinUntil<K, F: Future, U: Until<K, F::Output>> {
        #[pin]
        active: FuturesOrdered<KeyedFuture<K, F>>,
        output: HashMap<K, F::Output>,
        until: U
    }
}

pub(crate) trait Until<K, O> {
    fn until(&mut self, results: &HashMap<K, O>, cx: &mut Context<'_>) -> bool;
}

impl<K, O> Until<K, O> for usize {
    fn until(&mut self, results: &HashMap<K, O>, _cx: &mut Context<'_>) -> bool {
        results.len() >= *self
    }
}

impl<K, O, F: FnMut(&HashMap<K, O>, &mut Context<'_>) -> bool> Until<K, O> for F {
    fn until(&mut self, results: &HashMap<K, O>, cx: &mut Context<'_>) -> bool {
        self(results, cx)
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

pub(crate) fn join_until<K, F, I: IntoIterator<Item = (K, F)>, U: Until<K, F::Output>>(
    iter: I,
    until: U,
) -> JoinUntil<K, F, U>
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
        output: HashMap::with_capacity(active.len()),
        active,
        until,
    }
}

impl<K: Eq + Hash, F, U: Until<K, F::Output>> Future for JoinUntil<K, F, U>
where
    F: Future,
{
    type Output = HashMap<K, F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            if !this.until.until(this.output, cx) {
                match this.active.as_mut().poll_next(cx) {
                    Poll::Ready(Some((k, x))) => {
                        this.output.insert(k, x);
                        continue;
                    }
                    Poll::Ready(None) => {
                        // Done with all futures
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            return Poll::Ready(mem::take(this.output));
        }
    }
}
