use std::{sync::LazyLock, time::Duration};

use cosmic::{
    Element, Task, app,
    applet::{menu_button, padded_control},
    cosmic_theme::Spacing,
    iced::{self, Alignment, Subscription, window, mouse},
    theme,
    widget::{self, Column, divider, text, mouse_area},
};
use cosmic_time::{Instant, Timeline, anim, chain, id, once_cell::sync::Lazy};
use iced::{
    platform_specific::shell::wayland::commands::popup::{destroy_popup, get_popup},
    widget::container,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use crate::caffeine::Caffeine;
use crate::fl;
use crate::ipc::{self, ClientMessage, IpcEvent, Role, SharedState, ToEngine};
use crate::timer::{self, Timer};

static STAY_AWAKE_CONTROLS: Lazy<id::Toggler> = Lazy::new(id::Toggler::unique);
static REMEMBER_STATE_CONTROLS: Lazy<id::Toggler> = Lazy::new(id::Toggler::unique);

const ID: &str = "net.tropicbliss.CosmicExtAppletCaffeine";

#[derive(Default)]
pub struct Window {
    core: cosmic::app::Core,
    popup: Option<window::Id>,
    caffeine: Caffeine,
    timeline: Timeline,
    timer: Timer,
    timer_string: Option<String>,
    secondary_window: Option<SecondaryWindow>,
    persistent_state: Config,
    /// This instance's role, settled once the IPC subscription elects it.
    role: Option<Role>,
    /// Handle to talk back to the IPC engine; `Some` once a role is settled.
    engine_tx: Option<UnboundedSender<ToEngine>>,
    /// The master's last-broadcast state. Authoritative when `role` is
    /// `Client`; also kept up to date on a master so a promoted client can
    /// re-assert it for real.
    mirrored_state: SharedState,
    has_mirror: bool,
}

enum SecondaryWindow {
    CustomDuration { minutes_text: String },
    Settings,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct Config {
    remember_state: bool,
    custom_duration_text_box: String,
    timer_state: Option<TimerDuration>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
enum TimerDuration {
    #[default]
    Infinite,
    CustomSeconds(u64),
}

#[derive(Clone, Debug)]
pub enum Message {
    PopupClosed(window::Id),
    ToggleCaffeine(bool),
    IconPressed(mouse::Button),
    Frame(Instant),
    Tick,
    SetTimer(u64),
    CustomDuration,
    SecondaryWindowBack,
    EnterCustomDuration(String),
    CustomDurationStart,
    RememberStateToggle(chain::Toggler, bool),
    Settings,
    Ipc(IpcEvent),
}

impl cosmic::Application for Window {
    type Executor = cosmic::SingleThreadExecutor;
    type Flags = ();
    type Message = Message;
    const APP_ID: &'static str = ID;

    fn core(&self) -> &cosmic::app::Core {
        &self.core
    }

    fn core_mut(&mut self) -> &mut cosmic::app::Core {
        &mut self.core
    }

    fn style(&self) -> Option<cosmic::iced_runtime::Appearance> {
        Some(cosmic::applet::style())
    }

    fn init(core: cosmic::app::Core, _flags: ()) -> (Self, app::Task<Message>) {
        let config: Config =
            confy::load(ID, None).expect("config file should be in a valid format");
        let window = Window {
            core,
            persistent_state: config,
            ..Default::default()
        };
        // Whether this instance ends up master or client is only known once
        // the IPC subscription elects it; see `Message::Ipc`.
        (window, Task::none())
    }

    fn on_close_requested(&self, id: window::Id) -> Option<Message> {
        Some(Message::PopupClosed(id))
    }

    fn update(&mut self, message: Self::Message) -> app::Task<Message> {
        match message {
            Message::PopupClosed(id) => {
                self.secondary_window = None;
                if self.popup.as_ref() == Some(&id) {
                    self.popup = None;
                }
            }
            Message::ToggleCaffeine(is_stay_awake) => {
                self.dispatch_command(ClientMessage::SetCaffeinated(is_stay_awake));
            }
            Message::IconPressed(button) => {
                // Handle different mouse button clicks
                match button {
                    mouse::Button::Right => {
                        // Right-click opens the popup menu
                        return if let Some(p) = self.popup.take() {
                            destroy_popup(p)
                        } else {
                            let new_id = window::Id::unique();
                            self.popup.replace(new_id);
                            self.timeline = Timeline::new();
                            let popup_settings = self.core.applet.get_popup_settings(
                                self.core.main_window_id().unwrap(),
                                new_id,
                                None,
                                None,
                                None,
                            );
                            get_popup(popup_settings)
                        };
                    }
                    mouse::Button::Left => {
                        // Left-click toggles caffeine state
                        let new_state = !self.effective_caffeinated();
                        self.dispatch_command(ClientMessage::SetCaffeinated(new_state));
                    }
                    _ => {
                        // Ignore other buttons
                    }
                }
            }
            Message::Frame(now) => self.timeline.now(now),
            Message::Tick => match self.role {
                Some(Role::Master) => {
                    self.timer.tick();
                    self.timer_string = self.timer.get_formatted_time();
                    if self.timer.timer_just_ended() {
                        self.persistent_state.timer_state = None;
                        self.update_config();
                        self.set_stay_awake(false);
                        self.broadcast_state();
                    }
                }
                _ => {
                    self.timer_string = self.mirrored_remaining_seconds().map(timer::format_duration);
                }
            },
            Message::SetTimer(minutes) => {
                self.dispatch_command(ClientMessage::SetTimerMinutes(minutes));
            }
            Message::CustomDuration => {
                self.secondary_window = Some(SecondaryWindow::CustomDuration {
                    minutes_text: self.persistent_state.custom_duration_text_box.clone(),
                });
            }
            Message::SecondaryWindowBack => {
                self.secondary_window = None;
            }
            Message::EnterCustomDuration(input) => {
                if let Some(SecondaryWindow::CustomDuration { minutes_text }) =
                    &mut self.secondary_window
                {
                    if input.is_empty() || input.parse::<u32>().is_ok() {
                        *minutes_text = input.clone();
                        self.persistent_state.custom_duration_text_box = input;
                        self.update_config();
                    }
                }
            }
            Message::CustomDurationStart => {
                if let Some(SecondaryWindow::CustomDuration { minutes_text }) =
                    &self.secondary_window
                {
                    if minutes_text.is_empty() {
                        return Task::none();
                    }
                    let seconds = minutes_text.parse::<u64>().expect("valid u64 minutes text") * 60;
                    self.secondary_window = None;
                    self.dispatch_command(ClientMessage::StartCustomDurationSeconds(seconds));
                }
            }
            Message::RememberStateToggle(chain, remember_state) => {
                // The widget already hands us the right animation chain for
                // this, direct, user-driven change — unlike the stay-awake
                // toggle, which can also change programmatically (icon
                // click, remote command) and so reconstructs its own chain.
                self.timeline.set_chain(chain).start();
                match self.role {
                    Some(Role::Master) => {
                        self.persistent_state.remember_state = remember_state;
                        self.update_config();
                        self.broadcast_state();
                    }
                    _ => self.send_to_engine(ToEngine::SendCommand(ClientMessage::SetRememberState(
                        remember_state,
                    ))),
                }
            }
            Message::Settings => {
                self.secondary_window = Some(SecondaryWindow::Settings);
            }
            Message::Ipc(event) => self.handle_ipc_event(event),
        }
        Task::none()
    }

    fn view(&'_ self) -> Element<'_, Self::Message> {
        const ICON_EMPTY: &str = "net.tropicbliss.CosmicExtAppletCaffeine-empty";
        const ICON_FULL: &str = "net.tropicbliss.CosmicExtAppletCaffeine-full";

        let icon = if self.effective_caffeinated() {
            ICON_FULL
        } else {
            ICON_EMPTY
        };

        mouse_area(self.core.applet.icon_button(icon))
            .on_press(Message::IconPressed(mouse::Button::Left))
            .on_right_press(Message::IconPressed(mouse::Button::Right))
            .into()
    }

    fn view_window(&'_ self, _id: window::Id) -> Element<'_, Message> {
        static PRESET_MINUTES: LazyLock<Vec<(String, u64)>> = LazyLock::new(|| {
            vec![
                (fl!("fifteen-minutes"), 15),
                (fl!("thirty-minutes"), 30),
                (fl!("one-hour"), 60 * 1),
                (fl!("two-hours"), 60 * 2),
                (fl!("four-hours"), 60 * 4),
            ]
        });
        let mut content = Column::new();
        let Spacing {
            space_xxs, space_s, ..
        } = theme::active().cosmic().spacing;
        if matches!(
            &self.secondary_window,
            Some(SecondaryWindow::CustomDuration { .. }) | None
        ) {
            let mut stay_awake_text = fl!("stay-awake").to_string();
            if let Some(formatted_time) = &self.timer_string {
                stay_awake_text.push_str(&format!(" ({formatted_time})"))
            }
            content = content.push(padded_control(anim!(
                STAY_AWAKE_CONTROLS,
                &self.timeline,
                stay_awake_text,
                self.effective_caffeinated(),
                |_chain, enable| { Message::ToggleCaffeine(enable) }
            )));
            content = content
                .push(padded_control(divider::horizontal::default()).padding([space_xxs, space_s]));
        }
        match &self.secondary_window {
            Some(SecondaryWindow::CustomDuration { minutes_text }) => {
                content = content.push(padded_control(
                    widget::text_input("", minutes_text)
                        .label(fl!("minutes"))
                        .on_input(Message::EnterCustomDuration),
                ));
                content = content.push(
                    menu_button(text::body(fl!("start"))).on_press(Message::CustomDurationStart),
                );
            }
            Some(SecondaryWindow::Settings) => {
                content = content.push(padded_control(anim!(
                    REMEMBER_STATE_CONTROLS,
                    &self.timeline,
                    fl!("remember-state"),
                    self.effective_remember_state(),
                    Message::RememberStateToggle
                )));
                content = content.push(
                    padded_control(divider::horizontal::default()).padding([space_xxs, space_s]),
                );
            }
            None => {
                for (translation, minutes) in PRESET_MINUTES.iter() {
                    content = content.push(
                        menu_button(text::body(translation)).on_press(Message::SetTimer(*minutes)),
                    );
                }
                content = content.push(
                    menu_button(text::body(fl!("custom-duration")))
                        .on_press(Message::CustomDuration),
                );
                content = content.push(
                    padded_control(divider::horizontal::default()).padding([space_xxs, space_s]),
                );
                content = content
                    .push(menu_button(text::body(fl!("settings"))).on_press(Message::Settings));
            }
        }
        if matches!(
            &self.secondary_window,
            Some(SecondaryWindow::CustomDuration { .. } | SecondaryWindow::Settings)
        ) {
            content = content
                .push(menu_button(text::body(fl!("back"))).on_press(Message::SecondaryWindowBack));
        }
        content = content.align_x(Alignment::Start).padding([8, 0]);
        self.core.applet.popup_container(container(content)).into()
    }

    fn subscription(&self) -> Subscription<Message> {
        let mut subs = vec![
            self.timeline
                .as_subscription()
                .map(|(_, now)| Message::Frame(now)),
            ipc::subscription().map(Message::Ipc),
        ];
        let timer_active = match self.role {
            Some(Role::Master) => self.timer.is_started(),
            Some(Role::Client) => self.mirrored_state.timer_end_millis.is_some(),
            None => false,
        };
        if timer_active {
            subs.push(cosmic::iced::time::every(Duration::from_millis(500)).map(|_| Message::Tick));
        }
        Subscription::batch(subs)
    }

    fn on_app_exit(&mut self) -> Option<Message> {
        if self.role == Some(Role::Master) {
            if let Err(e) = self.caffeine.cleanup() {
                tracing::error!("Failed to exit gracefully: {e:?}");
            }
            let _ = std::fs::remove_file(ipc::socket_path());
        }
        None
    }
}

impl Window {
    fn update_config(&self) {
        confy::store(ID, None, &self.persistent_state).expect("cannot modify config");
    }

    /// The caffeinated/timer state this instance should render, whichever
    /// role it holds.
    fn effective_caffeinated(&self) -> bool {
        match self.role {
            Some(Role::Master) => self.caffeine.is_caffeinated(),
            _ => self.mirrored_state.caffeinated,
        }
    }

    fn effective_remember_state(&self) -> bool {
        match self.role {
            Some(Role::Master) => self.persistent_state.remember_state,
            _ => self.mirrored_state.remember_state,
        }
    }

    fn mirrored_remaining_seconds(&self) -> Option<u64> {
        self.mirrored_state
            .timer_end_millis
            .map(|end_ms| ((end_ms - ipc::epoch_millis_now()).max(0) / 1000) as u64)
    }

    fn send_to_engine(&self, message: ToEngine) {
        if let Some(tx) = &self.engine_tx {
            let _ = tx.send(message);
        }
    }

    /// Applies a state-changing action as the master would, then broadcasts
    /// the result. As a client, forwards it to the master instead.
    fn dispatch_command(&mut self, cmd: ClientMessage) {
        match self.role {
            Some(Role::Master) => self.apply_master_command(cmd),
            _ => self.send_to_engine(ToEngine::SendCommand(cmd)),
        }
    }

    /// Applies a command as the master: this is reached both for this
    /// instance's own local actions and for commands forwarded by clients.
    fn apply_master_command(&mut self, cmd: ClientMessage) {
        match cmd {
            ClientMessage::SetCaffeinated(on) => {
                self.persistent_state.timer_state = if on {
                    Some(TimerDuration::Infinite)
                } else {
                    None
                };
                self.update_config();
                self.set_stay_awake(on);
            }
            ClientMessage::SetTimerMinutes(minutes) => {
                let seconds = minutes * 60;
                self.timer.start(Duration::from_secs(seconds));
                self.persistent_state.timer_state = Some(TimerDuration::CustomSeconds(seconds));
                self.update_config();
                self.set_stay_awake(true);
            }
            ClientMessage::StartCustomDurationSeconds(seconds) => {
                self.timer.start(Duration::from_secs(seconds));
                self.persistent_state.timer_state = Some(TimerDuration::CustomSeconds(seconds));
                self.update_config();
                self.set_stay_awake(true);
            }
            ClientMessage::SetRememberState(remember) => {
                let changed = self.persistent_state.remember_state != remember;
                self.persistent_state.remember_state = remember;
                self.update_config();
                if changed {
                    self.animate_remember_state_toggle(remember);
                }
            }
        }
        self.timer_string = self.timer.get_formatted_time();
        self.broadcast_state();
    }

    fn broadcast_state(&self) {
        self.send_to_engine(ToEngine::Broadcast(self.shared_state()));
    }

    fn shared_state(&self) -> SharedState {
        SharedState {
            caffeinated: self.caffeine.is_caffeinated(),
            timer_end_millis: self
                .timer
                .remaining()
                .map(|remaining| ipc::epoch_millis_now() + remaining.as_millis() as i64),
            remember_state: self.persistent_state.remember_state,
        }
    }

    fn handle_ipc_event(&mut self, event: IpcEvent) {
        match event {
            IpcEvent::Elected(Role::Master, tx) => {
                let promoted = self.has_mirror;
                tracing::info!(
                    "Elected master{}",
                    if promoted { " (promoted after the previous master exited)" } else { "" }
                );
                self.engine_tx = Some(tx);
                self.role = Some(Role::Master);
                if promoted {
                    self.promote_from_mirror();
                } else {
                    self.restore_from_config();
                }
                self.timer_string = self.timer.get_formatted_time();
                self.broadcast_state();
            }
            IpcEvent::Elected(Role::Client, tx) => {
                tracing::info!("Connected to master as a client");
                self.engine_tx = Some(tx);
                self.role = Some(Role::Client);
            }
            IpcEvent::RemoteCommand(cmd) => {
                if self.role == Some(Role::Master) {
                    self.apply_master_command(cmd);
                }
            }
            IpcEvent::StateFromMaster(state) => {
                let caffeinated_changed = self.mirrored_state.caffeinated != state.caffeinated;
                let remember_changed = self.mirrored_state.remember_state != state.remember_state;
                self.has_mirror = true;
                self.mirrored_state = state;
                if caffeinated_changed {
                    self.animate_stay_awake_toggle(self.mirrored_state.caffeinated);
                }
                if remember_changed {
                    self.animate_remember_state_toggle(self.mirrored_state.remember_state);
                }
                self.timer_string = self.mirrored_remaining_seconds().map(timer::format_duration);
            }
            IpcEvent::Disconnected => {
                self.engine_tx = None;
                self.role = None;
            }
        }
    }

    /// First boot as master: restore the previous session's state from disk.
    fn restore_from_config(&mut self) {
        if self.persistent_state.remember_state {
            if let Some(timer_duration) = self.persistent_state.timer_state.clone() {
                if let TimerDuration::CustomSeconds(secs) = timer_duration {
                    self.timer.start(Duration::from_secs(secs));
                }
                self.set_stay_awake(true);
            }
        }
    }

    /// Promoted from client to master (the previous master exited): this
    /// instance never held a real D-Bus inhibitor, so re-assert the state it
    /// was mirroring for real before anyone notices.
    fn promote_from_mirror(&mut self) {
        let mirror = self.mirrored_state.clone();
        self.persistent_state.remember_state = mirror.remember_state;
        self.persistent_state.timer_state = if mirror.caffeinated {
            match mirror.timer_end_millis {
                Some(end_ms) => {
                    let remaining_secs = ((end_ms - ipc::epoch_millis_now()).max(0) / 1000) as u64;
                    self.timer.start(Duration::from_secs(remaining_secs));
                    Some(TimerDuration::CustomSeconds(remaining_secs))
                }
                None => Some(TimerDuration::Infinite),
            }
        } else {
            None
        };
        self.update_config();
        self.set_stay_awake(mirror.caffeinated);
    }

    fn animate_stay_awake_toggle(&mut self, enabled: bool) {
        let chain = if enabled {
            chain::Toggler::on(STAY_AWAKE_CONTROLS.clone(), 1.)
        } else {
            chain::Toggler::off(STAY_AWAKE_CONTROLS.clone(), 1.)
        };
        self.timeline.set_chain(chain).start();
    }

    fn animate_remember_state_toggle(&mut self, enabled: bool) {
        let chain = if enabled {
            chain::Toggler::on(REMEMBER_STATE_CONTROLS.clone(), 1.)
        } else {
            chain::Toggler::off(REMEMBER_STATE_CONTROLS.clone(), 1.)
        };
        self.timeline.set_chain(chain).start();
    }

    fn set_stay_awake(&mut self, is_stay_awake: bool) {
        tracing::info!(
            "{} stay awake",
            if is_stay_awake {
                "Enabling"
            } else {
                "Disabling"
            }
        );
        let changed = self.caffeine.is_caffeinated() != is_stay_awake;
        let result = if is_stay_awake {
            self.caffeine.caffeinate()
        } else {
            self.timer.cancel();
            self.timer_string = None;
            self.caffeine.decaffeinate()
        };
        match result {
            Ok(()) if changed => self.animate_stay_awake_toggle(is_stay_awake),
            Err(e) => tracing::error!("Failed to stay awake: {e:?}"),
            _ => {}
        }
    }
}
