use futures::{stream::FuturesOrdered, FutureExt, Stream};
use pin_project_lite::pin_project;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

pin_project! {
    /// Future for the [`join_n`] function.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct JoinN<K, F: Future> {
        #[pin]
        active: FuturesOrdered<KeyedFuture<K, F>>,
        output: HashMap<K, F::Output>,
        n: usize,
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

pub fn join_n<K, F, I: IntoIterator<Item = (K, F)>>(iter: I, n: usize) -> JoinN<K, F>
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

    assert!(active.len() >= n);

    JoinN {
        active,
        output: HashMap::with_capacity(n),
        n,
    }
}

impl<K: Eq + Hash, F> Future for JoinN<K, F>
where
    F: Future,
{
    type Output = HashMap<K, F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        Poll::Ready(loop {
            if let Some((k, x)) = ready!(this.active.as_mut().poll_next(cx)) {
                this.output.insert(k, x);
                if this.output.len() < *this.n {
                    continue;
                }
            }
            break mem::take(this.output);
        })
    }
}
