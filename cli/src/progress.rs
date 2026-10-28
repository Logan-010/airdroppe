use indicatif::{ProgressBar, ProgressStyle};

pub fn progress_bar(size: u64) -> ProgressBar {
    let pb = ProgressBar::new(size);
    pb.set_style(
                ProgressStyle::default_bar()
                    .template("{prefix} {spinner} [{elapsed_precise}] [{wide_bar}] {percent_precise} ({bytes_per_sec}, {eta})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
    pb
}
