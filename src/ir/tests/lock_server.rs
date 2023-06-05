use super::super::Client;

#[cfg(test)]
mod tests {
    #[test]
    fn lock_server() {
        enum Op {
            Lock,
            Unlock,
        }

        enum Res {
            Ok,
            No,
        }

        let client = Client::<Op, Res>::new();
    }
}
