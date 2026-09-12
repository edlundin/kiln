mod app;
mod components;
mod connection;
mod conversation;
mod theme;

use std::sync::Arc;

use gpui::{AppContext, Bounds, TitlebarOptions, WindowBounds, WindowOptions, px, size};
use gpui_component::Root;

gpui::actions!(kiln, [Quit]);

fn main() {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => Arc::new(runtime),
        Err(_) => {
            eprintln!("Kiln could not start its connection runtime.");
            return;
        }
    };
    let config = connection::ConnectionConfig {
        address: std::env::var("KILN_DESKTOP_ADDRESS").unwrap_or_default(),
        token_file: std::env::var("KILN_DESKTOP_TOKEN_FILE").unwrap_or_default(),
        repository_path: std::env::var("KILN_DESKTOP_REPOSITORY").unwrap_or_default(),
        session_id: std::env::var("KILN_DESKTOP_SESSION").unwrap_or_default(),
    };
    gpui_platform::application().run(move |cx| {
        gpui_component::init(cx);
        theme::apply(cx);
        cx.bind_keys([gpui::KeyBinding::new("cmd-q", Quit, None)]);
        cx.on_action(|_: &Quit, cx| cx.quit());
        cx.on_window_closed(|cx, _| {
            if cx.windows().is_empty() {
                cx.quit();
            }
        })
        .detach();
        let bounds = Bounds::centered(None, size(px(1060.), px(800.)), cx);
        cx.spawn(async move |cx| {
            let opened = cx.open_window(
                WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(bounds)),
                    window_min_size: Some(size(px(640.), px(520.))),
                    titlebar: Some(TitlebarOptions {
                        title: Some("Kiln".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                |window, cx| {
                    let view = cx.new(|cx| app::Desktop::new(config, runtime, window, cx));
                    cx.new(|cx| Root::new(view, window, cx))
                },
            );
            if opened.is_err() {
                eprintln!("Kiln could not open its desktop window.");
                cx.update(|cx| cx.quit());
            } else {
                cx.update(|cx| cx.activate(true));
            }
        })
        .detach();
    });
}
