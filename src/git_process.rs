#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

pub fn new_std_git_command() -> std::process::Command {
    let mut command = std::process::Command::new("git");
    configure_no_window(&mut command);
    command
}

pub fn new_tokio_git_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new("git");
    configure_no_window(command.as_std_mut());
    command
}

#[cfg(windows)]
fn configure_no_window(command: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;

    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn configure_no_window(_command: &mut std::process::Command) {}
