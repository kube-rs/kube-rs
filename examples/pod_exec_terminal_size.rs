use futures::{channel::mpsc::Sender, SinkExt, StreamExt};
use k8s_openapi::api::core::v1::Pod;

use kube::{
    api::{
        Api, AttachParams, AttachedProcess, DeleteParams, PostParams, ResourceExt, TerminalSize,
    },
    Client, runtime::wait::{await_condition, conditions::is_pod_running},
};
use tokio::{io::AsyncWriteExt, select};
use crossterm::event::{EventStream, Event};

// send the new terminal size to channel when it change
async fn handle_terminal_size(mut channel: Sender<TerminalSize>) -> Result<(), anyhow::Error> {
    // get event stream to get resize event
    let mut reader = EventStream::new();

    let (width, height) = crossterm::terminal::size()?;
    channel
        .send(TerminalSize { height, width })
        .await?;

    loop {
        // wait for a change
        let maybe_event = reader.next().await;
        match maybe_event {
            Some(Ok(Event::Resize(width, height))) => {
                channel
                .send(TerminalSize { height, width })
                .await?
            },
            // we don't care about other events type
            Some(Ok(_)) => {},
            Some(Err(err)) => {
                return Err(err.into());
            }
            None => break,
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;

    let pods: Api<Pod> = Api::default_namespaced(client);
    let p: Pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "example" },
        "spec": {
            "containers": [{
                "name": "example",
                "image": "alpine",
                // Do nothing
                "command": ["tail", "-f", "/dev/null"],
            }],
        }
    }))?;
    // Create pod if don't exist
    pods.create(&PostParams::default(), &p).await?;

    // Wait until the pod is running, otherwise we get 500 error.
    let running = await_condition(pods.clone(), "example", is_pod_running());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(15), running).await?;

    {
        // Here we we put the teerminal in 'raw' mode to directly get the input from the user and sending it to the server and getting the result from the server to disploy directly.
        // We also watch for change in your terminal size and send it to the server so that application that use the size work properly.
        crossterm::terminal::enable_raw_mode()?;
        let mut attached: AttachedProcess = pods
            .exec(
                "example",
                vec!["sh"],
                &AttachParams::default().stdin(true).tty(true).stderr(false),
            )
            .await?;

        let mut stdin = tokio_util::io::ReaderStream::new(tokio::io::stdin());
        let mut stdout = tokio::io::stdout();

        let mut output = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
        let mut input = attached.stdin().unwrap();

        let term_tx = attached.terminal_size().unwrap();

        let mut handle_terminal_size_handle = tokio::spawn(handle_terminal_size(term_tx));

        loop {
            select! {
                message = stdin.next() => {
                    match message {
                        Some(Ok(message)) => {
                            input.write(&message).await?;
                        }
                        _ => {
                            break;
                        },
                    }
                },
                message = output.next() => {
                    match message {
                        Some(Ok(message)) => {
                            stdout.write(&message).await?;
                            stdout.flush().await?;
                        },
                        _ => {
                            break
                        },
                    }
                },
                result = &mut handle_terminal_size_handle => {
                    match result {
                        Ok(_) => println!("End of terminal size stream"),
                        Err(e) => println!("Error getting terminal size: {e:?}")
                    }
                },
            };
        }
        crossterm::terminal::disable_raw_mode()?;
    }

    // Delete it
    pods.delete("example", &DeleteParams::default())
        .await?
        .map_left(|pdel| {
            assert_eq!(pdel.name_any(), "example");
        });

    println!("");
    Ok(())
}
