use std::time::{Duration, Instant};

/// Formats a number of seconds as `HH:MM:SS` or `MM:SS`.
pub fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{secs:02}")
    } else {
        format!("{minutes:02}:{secs:02}")
    }
}

pub struct Timer {
    ending_time: Option<Instant>,
    just_completed: bool,
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer {
    pub fn new() -> Self {
        Self {
            ending_time: None,
            just_completed: false,
        }
    }

    pub fn start(&mut self, duration: Duration) {
        self.ending_time = Some(Instant::now() + duration);
    }

    /// Time remaining until the timer ends, or `None` if it isn't running.
    pub fn remaining(&self) -> Option<Duration> {
        self.ending_time
            .map(|ending_time| ending_time.saturating_duration_since(Instant::now()))
    }

    pub fn get_formatted_time(&self) -> Option<String> {
        self.remaining().map(|remaining| format_duration(remaining.as_secs()))
    }

    pub fn is_started(&self) -> bool {
        self.ending_time.is_some()
    }

    pub fn timer_just_ended(&mut self) -> bool {
        let result = self.just_completed;
        if self.just_completed {
            self.just_completed = false;
        }
        result
    }

    pub fn cancel(&mut self) {
        self.ending_time = None;
    }

    pub fn tick(&mut self) {
        if let Some(ending_time) = &self.ending_time {
            if Instant::now() > *ending_time {
                self.ending_time = None;
                self.just_completed = true;
            }
        }
    }
}
