use futures::future;
use futures::future::FutureExt;
use log::debug;
use simplelog::{ConfigBuilder, LevelFilter, SimpleLogger};
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::delay_for;

fn receives_closure<F>(closure: F)
where
    F: Fn(i32) -> i32,
{
    let result = closure(1);
    debug!("closure(1) => {}", result);
}

fn returns_closure() -> impl Fn(i32) -> i32 {
    |x| x + 4
}

fn curry<F>(f: F, x: i32) -> impl Fn(i32) -> i32
where
    F: Fn(i32, i32) -> i32,
{
    move |y| f(x, y)
}

fn generic_curry<F, X, Y, Z>(f: F, x: X) -> impl Fn(Y) -> Z
where
    F: Fn(X, Y) -> Z,
    X: Copy,
{
    move |y| f(x, y)
}

fn returns_impl_future_i32() -> impl Future<Output = i32> {
    future::ready(42)
}

fn returns_dyn_future_i32() -> Pin<Box<dyn Future<Output = i32>>> {
    if rand::random() {
        Box::pin(future::ready(42))
    } else {
        Box::pin(future::lazy(|_| 1337))
    }
}

fn returns_future_result() -> impl Future<Output = Result<i32, impl Error>> {
    future::ok::<_, Infallible>(42) // the _ is inferred from the parameter type
}

fn returns_future_result_dyn_error() -> impl Future<Output = Result<i32, Box<dyn Error>>> {
    future::ok(42)
}

fn returns_delayed_future() -> impl Future<Output = i32> {
    delay_for(Duration::from_millis(500)).then(|_| futures::future::ready(42))
}

fn wait_a_sec<F, O>(f: F) -> impl Future<Output = O>
where
    F: Future<Output = O>,
{
    let delay = Duration::from_millis(1000);
    delay_for(delay).then(|_| f)
}

fn returns_future_chain() -> impl Future<Output = ()> {
    future::lazy(|_| debug!("in returns_future_chain()"))
        .then(|_| {
            debug!("in first then");
            future::ready("Hello from rt.block_on()")
        })
        .inspect(|result| debug!("future::ready() -> {}", result))
        .then(|_| returns_impl_future_i32())
        .inspect(|result| debug!("returns_impl_future_i32() -> {}", result))
        .then(|_| returns_dyn_future_i32())
        .inspect(|result| debug!("returns_dyn_future_i32() -> {}", result))
        .then(|_| returns_future_result())
        .map(|result| result.unwrap())
        .inspect(|result| debug!("returns_future_result().unwrap() -> {}", result))
        .then(|_| returns_future_result_dyn_error())
        .map(|result| result.unwrap())
        .inspect(|result| debug!("returns_future_result_dyn_error().unwrap() -> {}", result))
        .then(|_| returns_delayed_future())
        .inspect(|result| debug!("returns_delayed_future() -> {}", result))
        .then(|_| wait_a_sec(future::ready(42)))
        .inspect(|result| debug!("wait_a_sec(future::ready(42)) -> {}", result))
        .then(|_| {
            debug!("in last then");
            future::ready(())
        })
}

async fn async_hello() {
    debug!("Hello, asynchronously!");
}

async fn async_returns_i32() -> i32 {
    42
}

fn returns_future_i32() -> impl Future<Output = i32> {
    future::ready(42)
}

fn returns_async_block_i32() -> impl Future<Output = i32> {
    async { 42 }
}

async fn fallible() -> Result<(), Box<dyn Error>> {
    let _f = std::fs::File::open("foo.txt")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize simplelog logging
    let config = ConfigBuilder::new()
        .set_target_level(LevelFilter::Trace)
        .build();
    let _ = SimpleLogger::init(LevelFilter::Debug, config);

    {
        let y = 2;
        receives_closure(|x| x + y);
    }
    {
        let y = 3;
        receives_closure(|x| x + y);
    }
    {
        let closure = returns_closure();
        receives_closure(closure);
    }
    {
        let add = |x, y| x + y;
        let closure = curry(add, 5);
        receives_closure(closure);
    }
    {
        let two = 2;
        let add = |x, y| x + y + two;
        let closure = generic_curry(add, 4);
        receives_closure(closure);
    }
    {
        let concat = |s, t: &str| format!("{}{}", s, t);
        let closure = generic_curry(concat, "Hello, ");
        let result = closure("world!");
        debug!("{}", result);
    }

    tokio::spawn(future::lazy(|_| debug!("in tokio::spawn()")));
    debug!("in rt.block_on()");
    let r0 = future::ready("Hello from rt.block_on()").await;
    debug!("{}", r0);
    let r1 = returns_impl_future_i32().await;
    debug!("returns_impl_future_i32() -> {}", r1);
    let r2 = returns_dyn_future_i32().await;
    debug!("returns_dyn_future_i32() -> {}", r2);
    let r3 = returns_future_result().await;
    debug!("returns_future_result() -> {}", r3.unwrap());
    let r4 = returns_future_result_dyn_error().await;
    debug!("returns_future_result_dyn_error() -> {}", r4.unwrap());
    let r5 = returns_delayed_future().await;
    debug!("returns_delayed_future() -> {}", r5);
    let r6 = wait_a_sec(future::ready(42)).await;
    debug!("wait_a_sec(future::ready(42)) -> {}", r6);
    returns_future_chain().await;
    async_hello().await;
    let async_block = async {
        debug!("in async_block");
    };
    async_block.await;
    let x = 42;
    let async_capture = async {
        debug!("in async_capture, x => {}", x);
    };
    async_capture.await;
    let r7 = async_returns_i32().await;
    debug!("async_returns_i32 -> {}", r7);
    let r8 = returns_future_i32().await;
    debug!("returns_future_i32 -> {}", r8);
    let r9 = returns_async_block_i32().await;
    debug!("returns_async_block_i32 -> {}", r9);

    let _ = fallible().await?;
    Ok(())
}
