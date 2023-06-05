mod error;
mod message;
pub(crate) use error::Error;
pub(crate) use message::Message;

pub trait Transport {
    type Address;

    fn send<M: Message>(&self, message: M);
    fn receive<M>(&self) -> impl Future<Type = Result<Message, Error>>;
}
